#include "merger_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>

#define PIPE(fd) socketpair(AF_UNIX, SOCK_STREAM, PF_UNIX, fd)
#define MAX_LINES 100000

void build_args(operator_t *op, char **args) {
    switch (op->type) {
        case OP_SORT:   args[0] = "sort";   break;
        case OP_FILTER: args[0] = "filter"; break;
        case OP_UNIQUE: args[0] = "unique"; break;
        case OP_MERGER: args[0] = "merger"; break;
    }
    static char col_buf[16];
    sprintf(col_buf, "%d", op->column);
    args[1] = "-c";
    args[2] = col_buf;
    args[3] = "-t";
    switch(op->col_type) {
        case TYPE_TEXT: args[4] = "text"; break;
        case TYPE_NUM:  args[4] = "num"; break;
        case TYPE_DATE: args[4] = "date"; break;
    }
    int i = 5;
    if (op->type == OP_SORT && op->reverse) {
        args[i++] = "-r";
    }
    if (op->type == OP_FILTER) {
        switch(op->cmp) {
            case CMP_GT: args[i++] = "-g"; break;
            case CMP_LT: args[i++] = "-l"; break;
            case CMP_EQ: args[i++] = "-e"; break;
            case CMP_GE: args[i++] = "-ge"; break;
            case CMP_LE: args[i++] = "-le"; break;
            case CMP_NE: args[i++] = "-ne"; break;
        }
        args[i++] = op->cmp_value;
    }
    args[i++] = NULL;
}

void write_spec(merger_node_t *node, int fd) {
    dprintf(fd, "stdin %d\n", node->num_chains);
    for(int i = 0; i < node->num_chains; i++) {
        operator_chain_t *chain = &node->chains[i]; 
        if (chain->merger_child != NULL) {
            dprintf(fd, "%d %d merger\n", chain->start_line, chain->end_line);
            write_spec(chain->merger_child, fd);
        } else {
            dprintf(fd, "%d %d", chain->start_line, chain->end_line);
            for(int j = 0; j < chain->num_ops; j++) {
                char *args[16];
                build_args(&chain->ops[j], args);
                for (int k = 0; args[k] != NULL; k++) {
                    dprintf(fd, " %s", args[k]);
                }
                if (j < chain->num_ops - 1) {
                    dprintf(fd, " |");
                }
            }
            dprintf(fd, "\n");
        }
    }
}

void run_chain(operator_chain_t *chain, char **csv_lines, int num_lines, pid_t *exit_pids, int *exit_statuses, int *exit_count) {
    if (chain->merger_child != NULL) {
        int pipe_to_sub[2];
        int pipe_from_sub[2];

        PIPE(pipe_to_sub);
        PIPE(pipe_from_sub);

        pid_t child = fork();
        if (child == 0) {
            dup2(pipe_to_sub[0], STDIN_FILENO);
            dup2(pipe_from_sub[1], STDOUT_FILENO);
            close(pipe_to_sub[0]);
            close(pipe_to_sub[1]);
            close(pipe_from_sub[0]);
            close(pipe_from_sub[1]);
            char *args[] = {"merger", NULL};
            execvp("merger", args);
            exit(1);
        }

        close(pipe_to_sub[0]);
        close(pipe_from_sub[1]);

        pid_t helper = fork();
        if (helper == 0) {
            write_spec(chain->merger_child, pipe_to_sub[1]);
            for (int i = chain->start_line - 1; i <= chain->end_line - 1; i++) {
                write(pipe_to_sub[1], csv_lines[i], strlen(csv_lines[i]));
            }
            close(pipe_to_sub[1]);
            exit(0);
        } else {
            close(pipe_to_sub[1]);
        }

        char buf[MAX_LINE_SIZE];
        int n;
        while ((n = read(pipe_from_sub[0], buf, sizeof(buf))) > 0) {
            write(STDOUT_FILENO, buf, n);
        }
        close(pipe_from_sub[0]);

        waitpid(helper, NULL, 0);
        waitpid(child, NULL, 0);

        return;
    }
    
    int pipe_feed[2];
    int pipe_out[2];
    int pipes[MAX_OPERATORS][2]; 

    pid_t pids[MAX_OPERATORS];
    int pid_c = 0;

    PIPE(pipe_feed);
    PIPE(pipe_out);

    for(int i = 0; i < chain->num_ops - 1; i++) {
        PIPE(pipes[i]);
    }

    for(int i = 0; i < chain->num_ops; i++) {
        pid_t pid = fork();

        if(pid == 0) {
            int read_end = i == 0 ? pipe_feed[0] : pipes[i-1][0];
            dup2(read_end, STDIN_FILENO);

            int write_end = i == chain->num_ops - 1 ? pipe_out[1] : pipes[i][1];
            dup2(write_end, STDOUT_FILENO);

            close(pipe_feed[0]);
            close(pipe_feed[1]);

            close(pipe_out[0]);
            close(pipe_out[1]);

            for (int j = 0; j < chain->num_ops - 1; j++) {
                close(pipes[j][0]);
                close(pipes[j][1]);
            }

            char *args[16];
            build_args(&chain->ops[i], args);
            execvp(args[0], args);

            fprintf(stderr, "execvp failed\n");
            exit(1);
        }
        else {
            pids[pid_c++] = pid;
        }
    }

    close(pipe_feed[0]);
    close(pipe_out[1]);

    for (int i = 0; i < chain->num_ops - 1; i++) {
        close(pipes[i][0]);
        close(pipes[i][1]);
    }

    pid_t helper = fork();
    if (helper == 0) {
        for(int i = chain->start_line - 1; i <= chain->end_line - 1; i++) {
            write(pipe_feed[1], csv_lines[i], strlen(csv_lines[i]));
        }
        close(pipe_feed[1]);
        exit(0);
    }
    else {
        close(pipe_feed[1]);
    }

    char buf[MAX_LINE_SIZE];
    int n;
    while ((n = read(pipe_out[0], buf, sizeof(buf))) > 0) {
        write(STDOUT_FILENO, buf, n);
    }
    close(pipe_out[0]);

    waitpid(helper, NULL, 0);

    for (int i = 0; i < pid_c; i++) {
        int status;
        waitpid(pids[i], &status, 0);
        exit_pids[*exit_count] = pids[i];
        exit_statuses[*exit_count] = WEXITSTATUS(status);
        (*exit_count)++;
    }
}

int main(int argc, char **argv) {
    merger_node_t *root = parse_merger_input(stdin);
    if (!root) {
        fprintf(stderr, "merger: parse error\n");
        return 1;
    }

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "./tests/%s", root->filename);

    FILE *f = NULL;
    if (strcmp(root->filename, "stdin") == 0) {
        f = stdin;
    } else {
        f = fopen(filepath, "r");
        if (!f) {
            fprintf(stderr, "cannot open file\n");
            return 1;
        }
    }
    char *lines[MAX_LINES];
    int total = 0;

    char buf[MAX_LINE_SIZE];
    while (fgets(buf, sizeof(buf), f)) {
        lines[total] = malloc(strlen(buf) + 1);
        strcpy(lines[total], buf);
        total++;
    }

    if (f != stdin) {
        fclose(f);
    }

    pid_t exit_pids[MAX_OPERATORS * MAX_CHAINS];
    int exit_statuses[MAX_OPERATORS * MAX_CHAINS];
    int exit_count = 0;

    for (int i = 0; i < root->num_chains; i++) {
        run_chain(&root->chains[i], lines, total, exit_pids, exit_statuses, &exit_count);
    }

    for (int i = 0; i < exit_count; i++) {
        printf("EXIT-STATUS %d %d\n", exit_pids[i], exit_statuses[i]);
    }

    for (int i = 0; i < total; i++) {
        free(lines[i]);
    }

    free_merger_tree(root);
    return 0;
}
