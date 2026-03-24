#ifndef COMMON_H
#define COMMON_H

#define CHAIN_BUF_SIZE 10240
#define ELEMENT_MAX_SIZE 129
#define MAX_OPERATORS 32
#define MAX_CHAINS 64
#define MAX_LINE_SIZE 4096

typedef enum {
    TYPE_TEXT,
    TYPE_NUM,
    TYPE_DATE
} column_type_t;

typedef enum {
    CMP_GT,   /* > */
    CMP_LT,   /* < */
    CMP_EQ,   /* = */
    CMP_GE,   /* >= */
    CMP_LE,   /* <= */
    CMP_NE    /* != */
} filter_cmp_t;

#endif /* COMMON_H */
