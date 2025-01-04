QUERIES = {
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
}
