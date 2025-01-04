QUERIES = {
    "column_lineage": """
query Column($environmentId: BigInt!, $nodeUniqueId: String!, $filters: ColumnLineageFilter) {
    column(environmentId: $environmentId) {
        lineage(nodeUniqueId: $nodeUniqueId, filters: $filters) {
            nodeUniqueId
            relationship
        }
    }
}
""",
    "compiled_code": """
query Environment($environmentId: BigInt!, $filter: ModelAppliedFilter, $first: Int, $after: String) {
    environment(id: $environmentId) {
        applied {
            models(filter: $filter, first: $first, after: $after) {
                edges {
                    node {
                        compiledCode
                        uniqueId
                    }
                }
                pageInfo {
                    endCursor
                    hasNextPage
                    hasPreviousPage
                    startCursor
                }
                totalCount
            }
        }
    }
}
""",
    "node_lineage": """
query Environment($environmentId: BigInt!, $filter: LineageFilter!) {
    environment(id: $environmentId) {
        applied {
            lineage(filter: $filter) {
                uniqueId
            }
        }
    }
}
""",
}
