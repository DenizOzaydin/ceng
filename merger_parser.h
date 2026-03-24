#ifndef MERGER_PARSER_H
#define MERGER_PARSER_H

#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif
#include "common.h"

typedef enum {
    OP_SORT,
    OP_FILTER,
    OP_UNIQUE,
    OP_MERGER
} operator_type_t;

typedef struct {
    operator_type_t type;
    int column;
    column_type_t col_type;
    filter_cmp_t cmp;
    char cmp_value[ELEMENT_MAX_SIZE];
    int reverse;
} operator_t;

typedef struct merger_node merger_node_t;

typedef struct operator_chain {
    operator_t ops[MAX_OPERATORS];
    int num_ops;
    int start_line, end_line;
    merger_node_t *merger_child;  /* non-NULL when chain is "merger"; sub-input always follows */
} operator_chain_t;

struct merger_node {
    char filename[ELEMENT_MAX_SIZE];
    int has_filename;
    int start_line, end_line;
    operator_chain_t chains[MAX_CHAINS];
    int num_chains;
};

/**
 * Parses merger input from the given file.
 * Returns root merger_node_t or NULL on error.
 */
merger_node_t *parse_merger_input(FILE *f);

/**
 * Frees the merger tree.
 */
void free_merger_tree(merger_node_t *root);

/**
 * Prints the tree structure to the given FILE (e.g. stderr) for debugging.
 * Shows each merger node, its chains, and operator pipelines; recursion is shown by indentation.
 */
void print_merger_tree(const merger_node_t *root, FILE *out);

#ifdef __cplusplus
}
#endif

#endif /* MERGER_PARSER_H */