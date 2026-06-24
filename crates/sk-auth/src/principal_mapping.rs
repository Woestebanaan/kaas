//! `ssl.principal.mapping.rules` (gh #43, KIP-371).
//!
//! Port of `archive/internal/auth/principal_mapping.go`. Each rule is
//! one of:
//!
//! - `"DEFAULT"` — return the subject's CommonName unchanged.
//! - `"RULE:<regex>/<replacement>[/L|/U]"` — match the full DN
//!   against `<regex>`; on success, format the principal as
//!   `<replacement>` (with `$1`, `$2`, ... back-references). Optional
//!   `/L` lowercases, `/U` uppercases the result. First matching
//!   rule wins; if none match, fall through to the CN.
//!
//! Empty spec returns a mapper whose [`PrincipalMapper::apply`]
//! always returns the CN unchanged — preserves the pre-gh #43
//! behaviour so an empty config keeps clusters working.

use regex::Regex;

use crate::errors::AuthError;

#[derive(Debug, Default)]
pub struct PrincipalMapper {
    rules: Vec<Rule>,
}

#[derive(Debug)]
enum Rule {
    Default,
    Replace {
        regex: Regex,
        replacement: String,
        case_flag: CaseFlag,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaseFlag {
    None,
    Lower,
    Upper,
}

impl PrincipalMapper {
    /// Compile a rule string in Apache's format. Comma-separated;
    /// empty input returns a mapper whose `apply` always returns the
    /// CN unchanged.
    pub fn parse(spec: &str) -> Result<Self, AuthError> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Ok(Self::default());
        }
        let mut rules = Vec::new();
        for raw in split_rules(spec) {
            rules.push(parse_one(&raw)?);
        }
        Ok(Self { rules })
    }

    /// Return the mapped principal name for an X.509 subject. `cn` is
    /// the convenience-extracted CommonName the caller already has —
    /// used as the fall-back when no rule matches and on `DEFAULT`.
    pub fn apply(&self, subject_dn: &str, cn: &str) -> String {
        for rule in &self.rules {
            match rule {
                Rule::Default => return cn.to_owned(),
                Rule::Replace {
                    regex,
                    replacement,
                    case_flag,
                } => {
                    if let Some(caps) = regex.captures(subject_dn) {
                        let mut out = String::new();
                        caps.expand(replacement, &mut out);
                        return match case_flag {
                            CaseFlag::None => out,
                            CaseFlag::Lower => out.to_lowercase(),
                            CaseFlag::Upper => out.to_uppercase(),
                        };
                    }
                }
            }
        }
        cn.to_owned()
    }
}

/// Split the rule spec at rule boundaries. Rules are separated by
/// commas, but commas can appear inside a `RULE:`'s regex body
/// (Subject DNs use them as a separator). Split only at commas
/// immediately followed by `RULE:` or `DEFAULT`. Mirrors the Go
/// `splitRules` heuristic.
fn split_rules(spec: &str) -> Vec<String> {
    let bytes = spec.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let mut j = i;
        while j < bytes.len() {
            if bytes[j] == b',' {
                let mut k = j + 1;
                while k < bytes.len() && (bytes[k] == b' ' || bytes[k] == b'\t') {
                    k += 1;
                }
                if spec[k..].starts_with("RULE:") || spec[k..].starts_with("DEFAULT") {
                    break;
                }
            }
            j += 1;
        }
        out.push(spec[i..j].trim().to_owned());
        if j < bytes.len() {
            j += 1;
        }
        i = j;
    }
    out
}

fn parse_one(raw: &str) -> Result<Rule, AuthError> {
    if raw == "DEFAULT" {
        return Ok(Rule::Default);
    }
    let body = raw.strip_prefix("RULE:").ok_or_else(|| {
        AuthError::PrincipalMappingParse(format!(
            "rule {raw:?}: must start with RULE: or be DEFAULT"
        ))
    })?;

    let (body, case_flag) = if let Some(b) = body.strip_suffix("/L") {
        (b, CaseFlag::Lower)
    } else if let Some(b) = body.strip_suffix("/U") {
        (b, CaseFlag::Upper)
    } else if let Some(b) = body.strip_suffix('/') {
        (b, CaseFlag::None)
    } else {
        return Err(AuthError::PrincipalMappingParse(format!(
            "rule {raw:?}: must end with '/', '/L', or '/U'"
        )));
    };

    let slash = body.find('/').ok_or_else(|| {
        AuthError::PrincipalMappingParse(format!(
            "rule {raw:?}: missing '/' between regex and replacement"
        ))
    })?;
    let pattern = &body[..slash];
    let replacement = &body[slash + 1..];
    let regex = Regex::new(pattern)?;
    Ok(Rule::Replace {
        regex,
        replacement: replacement.to_owned(),
        case_flag,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_spec_returns_cn() {
        let m = PrincipalMapper::parse("").unwrap();
        assert_eq!(m.apply("CN=alice,OU=Engineering", "alice"), "alice");
    }

    #[test]
    fn default_returns_cn_unchanged() {
        let m = PrincipalMapper::parse("DEFAULT").unwrap();
        assert_eq!(m.apply("CN=AlIcE,OU=X", "AlIcE"), "AlIcE");
    }

    #[test]
    fn first_rule_wins_with_back_reference() {
        let m = PrincipalMapper::parse("RULE:^CN=([^,]+),.*$/$1/").unwrap();
        assert_eq!(m.apply("CN=alice,OU=Engineering,O=Acme", "alice"), "alice");
    }

    #[test]
    fn lower_case_flag_applied() {
        let m = PrincipalMapper::parse("RULE:^CN=([^,]+).*$/$1/L").unwrap();
        assert_eq!(m.apply("CN=Alice,OU=X", "Alice"), "alice");
    }

    #[test]
    fn upper_case_flag_applied() {
        let m = PrincipalMapper::parse("RULE:^CN=([^,]+).*$/$1/U").unwrap();
        assert_eq!(m.apply("CN=alice,OU=X", "alice"), "ALICE");
    }

    #[test]
    fn unmatched_rule_falls_through_to_cn() {
        let m = PrincipalMapper::parse("RULE:^OU=Eng.*$/eng-user/").unwrap();
        assert_eq!(m.apply("CN=alice,OU=Sales", "alice"), "alice");
    }

    #[test]
    fn split_rules_keeps_dn_commas_inside_regex() {
        // The pattern body contains a comma — must not be split there.
        let m = PrincipalMapper::parse("RULE:^CN=([^,]+),OU=([^,]+),.*$/$1@$2/L,DEFAULT").unwrap();
        assert_eq!(m.apply("CN=Alice,OU=ENG,O=Acme", "Alice"), "alice@eng");
    }

    #[test]
    fn invalid_rule_syntax_errors() {
        let err = PrincipalMapper::parse("RULE:bad-syntax").unwrap_err();
        assert!(matches!(err, AuthError::PrincipalMappingParse(_)));
    }

    #[test]
    fn invalid_regex_errors() {
        let err = PrincipalMapper::parse("RULE:[unclosed/x/").unwrap_err();
        assert!(matches!(err, AuthError::Regex(_)));
    }

    #[test]
    fn multiple_rules_first_match_wins() {
        let m = PrincipalMapper::parse(
            "RULE:^CN=admin,.*$/superuser/U,RULE:^CN=([^,]+),.*$/$1/L,DEFAULT",
        )
        .unwrap();
        assert_eq!(m.apply("CN=admin,OU=X", "admin"), "SUPERUSER");
        assert_eq!(m.apply("CN=Bob,OU=X", "Bob"), "bob");
        // Neither rule's regex matches the DN below; the explicit
        // DEFAULT returns the CN unchanged.
        assert_eq!(m.apply("OU=NoCNHere", "fallback-cn"), "fallback-cn");
    }
}
