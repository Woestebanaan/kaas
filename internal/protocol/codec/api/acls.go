package api

import "github.com/woestebanaan/skafka/internal/protocol/codec"

// AclFilter is used in DescribeAcls and DeleteAcls requests.
type AclFilter struct {
	ResourceTypeFilter int8
	ResourceNameFilter string // nullable
	PatternTypeFilter  int8   // v1+: 1=any,2=match,3=literal,4=prefixed
	PrincipalFilter    string // nullable
	HostFilter         string // nullable
	Operation          int8
	PermissionType     int8
}

// AclBinding describes a single ACL.
type AclBinding struct {
	ResourceType int8
	ResourceName string
	PatternType  int8 // v1+
	Principal    string
	Host         string
	Operation    int8
	Permission   int8
}

// DescribeAclsRequest (key 29, v0–v3).
type DescribeAclsRequest struct {
	AclFilter
}

// DescribeAclsResponse (key 29, v0–v3).
type DescribeAclsResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   string // nullable
	Resources      []DescribeAclsResource
}

type DescribeAclsResource struct {
	ResourceType int8
	ResourceName string
	PatternType  int8 // v1+
	ACLs         []MatchingACL
}

type MatchingACL struct {
	Principal   string
	Host        string
	Operation   int8
	Permission  int8
}

func DecodeDescribeAclsRequest(r *codec.Reader, version int16) (*DescribeAclsRequest, error) {
	req := &DescribeAclsRequest{}
	flexible := version >= 2
	var err error

	if req.ResourceTypeFilter, err = r.ReadInt8(); err != nil {
		return nil, err
	}
	rname, _, err := nullableString(r, flexible)
	if err != nil {
		return nil, err
	}
	req.ResourceNameFilter = rname
	if version >= 1 {
		if req.PatternTypeFilter, err = r.ReadInt8(); err != nil {
			return nil, err
		}
	}
	pf, _, err := nullableString(r, flexible)
	if err != nil {
		return nil, err
	}
	req.PrincipalFilter = pf
	hf, _, err := nullableString(r, flexible)
	if err != nil {
		return nil, err
	}
	req.HostFilter = hf
	if req.Operation, err = r.ReadInt8(); err != nil {
		return nil, err
	}
	if req.PermissionType, err = r.ReadInt8(); err != nil {
		return nil, err
	}
	if flexible {
		return req, r.ReadTaggedFields()
	}
	return req, nil
}

func EncodeDescribeAclsResponse(w *codec.Writer, resp *DescribeAclsResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)
	w.WriteInt16(resp.ErrorCode)
	if flexible {
		w.WriteCompactNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
	} else {
		w.WriteNullableString(resp.ErrorMessage, resp.ErrorMessage == "")
	}
	writeResources := func() {
		for _, res := range resp.Resources {
			w.WriteInt8(res.ResourceType)
			writeString(w, res.ResourceName, flexible)
			if version >= 1 {
				w.WriteInt8(res.PatternType)
			}
			writeACLs := func() {
				for _, a := range res.ACLs {
					writeString(w, a.Principal, flexible)
					writeString(w, a.Host, flexible)
					w.WriteInt8(a.Operation)
					w.WriteInt8(a.Permission)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(res.ACLs), writeACLs)
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteArray(len(res.ACLs), writeACLs)
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Resources), writeResources)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Resources), writeResources)
	}
}

// CreateAclsRequest (key 30, v0–v3).
type CreateAclsRequest struct {
	Creations []AclBinding
}

// CreateAclsResponse (key 30, v0–v3).
type CreateAclsResponse struct {
	ThrottleTimeMs int32
	Results        []CreateAclsResult
}

type CreateAclsResult struct {
	ErrorCode    int16
	ErrorMessage string // nullable
}

func DecodeCreateAclsRequest(r *codec.Reader, version int16) (*CreateAclsRequest, error) {
	req := &CreateAclsRequest{}
	flexible := version >= 2

	readBinding := func() error {
		var b AclBinding
		var err error
		if b.ResourceType, err = r.ReadInt8(); err != nil {
			return err
		}
		if b.ResourceName, err = readString(r, flexible); err != nil {
			return err
		}
		if version >= 1 {
			if b.PatternType, err = r.ReadInt8(); err != nil {
				return err
			}
		}
		if b.Principal, err = readString(r, flexible); err != nil {
			return err
		}
		if b.Host, err = readString(r, flexible); err != nil {
			return err
		}
		if b.Operation, err = r.ReadInt8(); err != nil {
			return err
		}
		if b.Permission, err = r.ReadInt8(); err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Creations = append(req.Creations, b)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readBinding); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readBinding)
}

func EncodeCreateAclsResponse(w *codec.Writer, resp *CreateAclsResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)
	writeResults := func() {
		for _, r := range resp.Results {
			w.WriteInt16(r.ErrorCode)
			if flexible {
				w.WriteCompactNullableString(r.ErrorMessage, r.ErrorMessage == "")
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteNullableString(r.ErrorMessage, r.ErrorMessage == "")
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.Results), writeResults)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.Results), writeResults)
	}
}

// DeleteAclsRequest (key 31, v0–v3).
type DeleteAclsRequest struct {
	Filters []AclFilter
}

// DeleteAclsResponse (key 31, v0–v3).
type DeleteAclsResponse struct {
	ThrottleTimeMs int32
	FilterResults  []DeleteAclsFilterResult
}

type DeleteAclsFilterResult struct {
	ErrorCode    int16
	ErrorMessage string // nullable
	MatchingACLs []DeleteAclsMatchingACL
}

type DeleteAclsMatchingACL struct {
	ErrorCode    int16
	ErrorMessage string // nullable
	AclBinding
}

func DecodeDeleteAclsRequest(r *codec.Reader, version int16) (*DeleteAclsRequest, error) {
	req := &DeleteAclsRequest{}
	flexible := version >= 2

	readFilter := func() error {
		var f AclFilter
		var err error
		if f.ResourceTypeFilter, err = r.ReadInt8(); err != nil {
			return err
		}
		rn, _, err := nullableString(r, flexible)
		if err != nil {
			return err
		}
		f.ResourceNameFilter = rn
		if version >= 1 {
			if f.PatternTypeFilter, err = r.ReadInt8(); err != nil {
				return err
			}
		}
		pf, _, err := nullableString(r, flexible)
		if err != nil {
			return err
		}
		f.PrincipalFilter = pf
		hf, _, err := nullableString(r, flexible)
		if err != nil {
			return err
		}
		f.HostFilter = hf
		if f.Operation, err = r.ReadInt8(); err != nil {
			return err
		}
		if f.PermissionType, err = r.ReadInt8(); err != nil {
			return err
		}
		if flexible {
			if err := r.ReadTaggedFields(); err != nil {
				return err
			}
		}
		req.Filters = append(req.Filters, f)
		return nil
	}
	if flexible {
		if err := r.ReadCompactArray(readFilter); err != nil {
			return nil, err
		}
		return req, r.ReadTaggedFields()
	}
	return req, r.ReadArray(readFilter)
}

func EncodeDeleteAclsResponse(w *codec.Writer, resp *DeleteAclsResponse, version int16) {
	flexible := version >= 2
	w.WriteInt32(resp.ThrottleTimeMs)
	writeFilters := func() {
		for _, f := range resp.FilterResults {
			w.WriteInt16(f.ErrorCode)
			if flexible {
				w.WriteCompactNullableString(f.ErrorMessage, f.ErrorMessage == "")
			} else {
				w.WriteNullableString(f.ErrorMessage, f.ErrorMessage == "")
			}
			writeMatching := func() {
				for _, m := range f.MatchingACLs {
					w.WriteInt16(m.ErrorCode)
					if flexible {
						w.WriteCompactNullableString(m.ErrorMessage, m.ErrorMessage == "")
					} else {
						w.WriteNullableString(m.ErrorMessage, m.ErrorMessage == "")
					}
					w.WriteInt8(m.ResourceType)
					writeString(w, m.ResourceName, flexible)
					if version >= 1 {
						w.WriteInt8(m.PatternType)
					}
					writeString(w, m.Principal, flexible)
					writeString(w, m.Host, flexible)
					w.WriteInt8(m.Operation)
					w.WriteInt8(m.Permission)
					if flexible {
						w.WriteEmptyTaggedFields()
					}
				}
			}
			if flexible {
				w.WriteCompactArray(len(f.MatchingACLs), writeMatching)
				w.WriteEmptyTaggedFields()
			} else {
				w.WriteArray(len(f.MatchingACLs), writeMatching)
			}
		}
	}
	if flexible {
		w.WriteCompactArray(len(resp.FilterResults), writeFilters)
		w.WriteEmptyTaggedFields()
	} else {
		w.WriteArray(len(resp.FilterResults), writeFilters)
	}
}
