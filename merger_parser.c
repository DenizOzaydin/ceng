#include "merger_parser.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static int parse_column_type(const char *s, column_type_t *out) {
    if (strcmp(s, "text") == 0) { *out = TYPE_TEXT; return 1; }
    if (strcmp(s, "num") == 0) { *out = TYPE_NUM; return 1; }
    if (strcmp(s, "date") == 0) { *out = TYPE_DATE; return 1; }
    return 0;
}

static int parse_filter_cmp(const char *s, filter_cmp_t *out) {
    if (strcmp(s, "-g") == 0) { *out = CMP_GT; return 1; }
    if (strcmp(s, "-l") == 0) { *out = CMP_LT; return 1; }
    if (strcmp(s, "-e") == 0) { *out = CMP_EQ; return 1; }
    if (strcmp(s, "-ge") == 0) { *out = CMP_GE; return 1; }
    if (strcmp(s, "-le") == 0) { *out = CMP_LE; return 1; }
    if (strcmp(s, "-ne") == 0) { *out = CMP_NE; return 1; }
    return 0;
}

static void skip_spaces(char **p) {
    while (**p && isspace((unsigned char)**p)) (*p)++;
}

static int next_token(char **p, char *buf, size_t buf_size) {
    skip_spaces(p);
    if (!**p) return 0;
    size_t i = 0;
    if (**p == '"') {
        (*p)++;
        while (**p && **p != '"' && i < buf_size - 1) buf[i++] = *(*p)++;
        if (**p == '"') (*p)++;
        buf[i] = '\0';
        return 1;
    }
    while (**p && !isspace((unsigned char)**p) && **p != '|' && i < buf_size - 1)
        buf[i++] = *(*p)++;
    buf[i] = '\0';
    return i > 0;
}

static int parse_operator_args(char *args_str, operator_t *op) {
    op->column = -1;
    op->col_type = TYPE_TEXT;
    op->cmp = CMP_EQ;
    op->cmp_value[0] = '\0';
    op->reverse = 0;

    char tok[ELEMENT_MAX_SIZE];
    char *p = args_str;

    while (next_token(&p, tok, sizeof(tok))) {
        if (strcmp(tok, "-c") == 0 || strcmp(tok, "--column") == 0) {
            if (!next_token(&p, tok, sizeof(tok))) return 0;
            op->column = atoi(tok);
        } else if (strcmp(tok, "-t") == 0 || strcmp(tok, "--type") == 0) {
            if (!next_token(&p, tok, sizeof(tok))) return 0;
            if (!parse_column_type(tok, &op->col_type)) return 0;
        } else if (strcmp(tok, "-r") == 0 || strcmp(tok, "--reverse") == 0) {
            op->reverse = 1;
        } else if (parse_filter_cmp(tok, &op->cmp)) {
            if (!next_token(&p, tok, sizeof(tok))) return 0;
            strncpy(op->cmp_value, tok, ELEMENT_MAX_SIZE - 1);
            op->cmp_value[ELEMENT_MAX_SIZE - 1] = '\0';
        }
    }
    return op->column >= 0;
}

static int parse_chain_line(char *line, operator_chain_t *chain, merger_node_t *node) {
    char *p = line;
    char tok[ELEMENT_MAX_SIZE];

    if (!next_token(&p, tok, sizeof(tok))) return 0;
    chain->start_line = atoi(tok);
    if (!next_token(&p, tok, sizeof(tok))) return 0;
    chain->end_line = atoi(tok);

    chain->num_ops = 0;
    chain->merger_child = NULL;

    char pipeline[CHAIN_BUF_SIZE];
    size_t pi = 0;
    skip_spaces(&p);
    while (*p && pi < CHAIN_BUF_SIZE - 1) pipeline[pi++] = *p++;
    pipeline[pi] = '\0';

    if (pi == 0) return 0;

    char *seg = pipeline;
    char *pipe_pos;
    while ((pipe_pos = strchr(seg, '|')) != NULL) {
        *pipe_pos = '\0';
        char op_name[ELEMENT_MAX_SIZE];
        char *sp = seg;
        if (!next_token(&sp, op_name, sizeof(op_name))) {
            seg = pipe_pos + 1;
            continue;
        }
        skip_spaces(&sp);

        if (chain->num_ops >= MAX_OPERATORS) return 0;

        if (strcmp(op_name, "merger") == 0) {
            chain->ops[chain->num_ops].type = OP_MERGER;
            chain->ops[chain->num_ops].column = 0;
            chain->num_ops++;
            chain->merger_child = (merger_node_t *)calloc(1, sizeof(merger_node_t));
            if (!chain->merger_child) return 0;
            chain->merger_child->has_filename = 0;
            return 1;
        }

        operator_type_t ot;
        if (strcmp(op_name, "sort") == 0) ot = OP_SORT;
        else if (strcmp(op_name, "filter") == 0) ot = OP_FILTER;
        else if (strcmp(op_name, "unique") == 0) ot = OP_UNIQUE;
        else return 0;

        chain->ops[chain->num_ops].type = ot;
        if (!parse_operator_args(sp, &chain->ops[chain->num_ops])) return 0;
        chain->num_ops++;
        seg = pipe_pos + 1;
    }

    if (chain->num_ops > 0 && chain->ops[0].type == OP_MERGER)
        return 1;

    char op_name[ELEMENT_MAX_SIZE];
    char *sp = seg;
    if (!next_token(&sp, op_name, sizeof(op_name))) return 0;
    skip_spaces(&sp);

    if (chain->num_ops >= MAX_OPERATORS) return 0;

    if (strcmp(op_name, "merger") == 0) {
        chain->ops[chain->num_ops].type = OP_MERGER;
        chain->ops[chain->num_ops].column = 0;
        chain->num_ops++;
        chain->merger_child = (merger_node_t *)calloc(1, sizeof(merger_node_t));
        if (!chain->merger_child) return 0;
        chain->merger_child->has_filename = 0;
        return 1;
    }

    operator_type_t ot;
    if (strcmp(op_name, "sort") == 0) ot = OP_SORT;
    else if (strcmp(op_name, "filter") == 0) ot = OP_FILTER;
    else if (strcmp(op_name, "unique") == 0) ot = OP_UNIQUE;
    else return 0;

    chain->ops[chain->num_ops].type = ot;
    if (!parse_operator_args(sp, &chain->ops[chain->num_ops])) return 0;
    chain->num_ops++;
    return 1;
}

static merger_node_t *parse_merger_recursive(FILE *f, int has_filename) {
    merger_node_t *node = (merger_node_t *)calloc(1, sizeof(merger_node_t));
    if (!node) return NULL;

    node->has_filename = has_filename;
    node->filename[0] = '\0';

    char line[CHAIN_BUF_SIZE];
    if (!fgets(line, sizeof(line), f)) {
        free(node);
        return NULL;
    }
    line[strcspn(line, "\n")] = '\0';

    char *p = line;
    char tok[ELEMENT_MAX_SIZE];

    if (!next_token(&p, tok, sizeof(tok))) {
        free(node);
        return NULL;
    }
    if (has_filename) {
        strncpy(node->filename, tok, ELEMENT_MAX_SIZE - 1);
        node->filename[ELEMENT_MAX_SIZE - 1] = '\0';
        if (!next_token(&p, tok, sizeof(tok))) {
            free(node);
            return NULL;
        }
        node->num_chains = atoi(tok);
    } else {
        node->num_chains = atoi(tok);
    }

    if (node->num_chains <= 0 || node->num_chains > MAX_CHAINS) {
        free(node);
        return NULL;
    }

    for (int i = 0; i < node->num_chains; i++) {
        if (!fgets(line, sizeof(line), f)) {
            free_merger_tree(node);
            return NULL;
        }
        line[strcspn(line, "\n")] = '\0';

        if (!parse_chain_line(line, &node->chains[i], node)) {
            free_merger_tree(node);
            return NULL;
        }

        /* Merger in a chain is always followed by sub-input (next line + num_chains lines) */
        if (node->chains[i].merger_child != NULL) {
            merger_node_t *sub = parse_merger_recursive(f, 0);
            if (!sub) {
                free_merger_tree(node);
                return NULL;
            }
            free(node->chains[i].merger_child);
            node->chains[i].merger_child = sub;
        }
    }
    return node;
}

merger_node_t *parse_merger_input(FILE *f) {
    return parse_merger_recursive(f, 1);
}

void free_merger_tree(merger_node_t *root) {
    if (!root) return;
    for (int i = 0; i < root->num_chains; i++) {
        if (root->chains[i].merger_child)
            free_merger_tree(root->chains[i].merger_child);
    }
    free(root);
}

static void print_chain(const operator_chain_t *c, FILE *out, int depth);
static void print_merger_tree_at(const merger_node_t *root, FILE *out, int depth);

static const char *op_type_str(operator_type_t t) {
    switch (t) {
        case OP_SORT: return "sort";
        case OP_FILTER: return "filter";
        case OP_UNIQUE: return "unique";
        case OP_MERGER: return "merger";
    }
    return "?";
}

static void print_chain(const operator_chain_t *c, FILE *out, int depth) {
    for (int d = 0; d < depth; d++) fprintf(out, "  ");
    fprintf(out, "chain [%d-%d]: ", c->start_line, c->end_line);
    if (c->merger_child) {
        fprintf(out, "merger (sub)\n");
        print_merger_tree_at(c->merger_child, out, depth + 1);
        return;
    }
    for (int j = 0; j < c->num_ops; j++) {
        if (j) fprintf(out, " | ");
        fprintf(out, "%s -c %d", op_type_str(c->ops[j].type), c->ops[j].column);
    }
    fprintf(out, "\n");
}

static void print_merger_tree_at(const merger_node_t *root, FILE *out, int depth) {
    if (!root) return;
    for (int d = 0; d < depth; d++) fprintf(out, "  ");
    if (root->has_filename && root->filename[0])
        fprintf(out, "merger file=%s chains=%d\n", root->filename, root->num_chains);
    else
        fprintf(out, "merger (sub) chains=%d\n", root->num_chains);
    for (int i = 0; i < root->num_chains; i++)
        print_chain(&root->chains[i], out, depth + 1);
}

void print_merger_tree(const merger_node_t *root, FILE *out) {
    print_merger_tree_at(root, out, 0);
}