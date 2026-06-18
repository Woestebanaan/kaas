package protocol

// apiNames maps Kafka API keys to the human-readable name used in
// metric / span attributes. Sourced from the Apache Kafka protocol
// guide (https://kafka.apache.org/protocol#protocol_api_keys); the
// list mirrors the keys the broker actually registers in
// internal/broker/broker.go. Unknown keys fall through to "Unknown".
//
// The map is consulted at Register time so the string is captured
// once per handler — zero hot-path allocation per request.
var apiNames = map[int16]string{
	0:  "Produce",
	1:  "Fetch",
	2:  "ListOffsets",
	3:  "Metadata",
	8:  "OffsetCommit",
	9:  "OffsetFetch",
	10: "FindCoordinator",
	11: "JoinGroup",
	12: "Heartbeat",
	13: "LeaveGroup",
	14: "SyncGroup",
	15: "DescribeGroups",
	16: "ListGroups",
	17: "SaslHandshake",
	18: "ApiVersions",
	19: "CreateTopics",
	20: "DeleteTopics",
	21: "DeleteRecords",
	22: "InitProducerId",
	24: "AddPartitionsToTxn",
	25: "AddOffsetsToTxn",
	26: "EndTxn",
	27: "WriteTxnMarkers",
	28: "TxnOffsetCommit",
	29: "DescribeAcls",
	30: "CreateAcls",
	31: "DeleteAcls",
	32: "DescribeConfigs",
	35: "DescribeLogDirs",
	36: "SaslAuthenticate",
	42: "DeleteGroups",
	60: "DescribeCluster",
}

// APIName returns the protocol name for the given key, falling back
// to "Unknown" for keys the broker doesn't register. Exported so
// other observability sites (tracing middleware, future log
// enrichment) can reuse the same name.
func APIName(apiKey int16) string {
	if name, ok := apiNames[apiKey]; ok {
		return name
	}
	return "Unknown"
}
