/*
    MIT License

    Copyright (c) 2024 winterrdog

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to
    deal in the Software without restriction, including without limitation the
    rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
    sell copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in
    all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    IN THE SOFTWARE.
*/
#include "db.h"

void print_row(row_t* r)
{
    printf("( %d, %s, %s )\n", r->id, r->username, r->email);
}

void* get_page(pager_t* pager, u32 page_num)
{
    void* page;
    u32 num_pages;
    off_t off;
    ssize_t bytes_read;

    if (page_num > TABLE_MAX_PAGES) {
        printf("tried to fetch a page out of bounds. %d > %d\n", page_num,
            TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if (!pager->pages[page_num]) {
        // cache miss. allocate memory and load from file
        page = xmalloc(PAGE_SIZE);
        num_pages = pager->file_len / PAGE_SIZE;

        // we might hv saved a partial page at the end of file
        if (pager->file_len % PAGE_SIZE != 0) {
            num_pages++;
        }

        // fetch page from file
        if (page_num <= num_pages) {
            off = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
            if (off < 0) {
                printf("failed to reposition for the current page.\n");
                exit(EXIT_FAILURE);
            }

            bytes_read = read(pager->fd, page, PAGE_SIZE);
            if (bytes_read < 0) {
                printf("failed to read in data from file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

void serialize_row(row_t* src, void* dest)
{
    // store id
    memcpy(dest + ID_OFFSET, &(src->id), ID_SIZE);

    // store username
    memcpy(dest + USERNAME_OFFSET, &(src->username), USERNAME_SIZE);

    // store email
    memcpy(dest + EMAIL_OFFSET, &(src->email), EMAIL_SIZE);
}

void deserialize_row(void* src, row_t* dest)
{
    // read id
    memcpy(&(dest->id), src + ID_OFFSET, ID_SIZE);

    // read username
    memcpy(&(dest->username), src + USERNAME_OFFSET, USERNAME_SIZE);

    // read email
    memcpy(&(dest->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}

cursor_t* table_start(table_t* table)
{
    cursor_t* cursor;
    u32 num_cells;
    void* root_node;

    cursor = xmalloc(sizeof(cursor_t));
    cursor->table = table, cursor->cell_num = 0x0,
    cursor->page_num = table->root_page_num;

    root_node = get_page(table->pager, table->root_page_num);
    num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

void* cursor_value(cursor_t* c)
{
    u32 page_num;
    void* page;

    page_num = c->page_num;
    page = get_page(c->table->pager, page_num);

    return leaf_node_value(page, c->cell_num);
}

void cursor_advance(cursor_t* c)
{
    u32 page_num;
    void* node;

    page_num = c->page_num;
    node = get_page(c->table->pager, page_num);
    c->cell_num++;
    if (c->cell_num < (*leaf_node_num_cells(node))) {
        return;
    }
    c->end_of_table = 0x1;
}

pager_t* pager_open(const char* fname)
{
    int fd;
    off_t file_len;
    pager_t* pager;
    u32 i;

    // open file in r/w mode or creating one if not existent
    // with read & write permissions for current user
    fd = open(fname, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    if (fd < 0) {
        printf("unable to open file, %d.\n", errno);
        exit(EXIT_FAILURE);
    }

    // set up pager
    file_len = lseek(fd, 0, SEEK_END);
    if ((file_len % PAGE_SIZE) != 0) {
        printf(
            "db file is not a whole number of pages. Corrupt database file.\n");
        exit(EXIT_FAILURE);
    }

    pager = xmalloc(sizeof(pager_t));
    pager->file_len = file_len, pager->fd = fd;
    pager->num_pages = (file_len / PAGE_SIZE);
    for (i = 0; i != TABLE_MAX_PAGES; ++i) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void pager_flush(pager_t* pager, u32 page_num)
{
    off_t offset;
    ssize_t bytes_written;

    if (!pager->pages[page_num]) {
        printf("tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);
    if (offset < 0) {
        printf("failed to reposition for the current page, %d.\n", errno);
        exit(EXIT_FAILURE);
    }

    bytes_written = write(pager->fd, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written < 0) {
        perror("error writing page cache to disk:");
        exit(EXIT_FAILURE);
    }
}

// B - T R E E  S P E C I F I C S
u32* leaf_node_num_cells(void* node)
{
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, u32 cell_num)
{
    return node + LEAF_NODE_HEADER_SIZE + (cell_num * LEAF_NODE_CELL_SIZE);
}

void init_leaf_node(void* node)
{
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);

    u32* num_cells = leaf_node_num_cells(node);
    *num_cells = 0x0;
}

void init_internal_node(void* node)
{
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);

    u32* num_keys = internal_node_num_keys(node);
    *num_keys = 0x0;
}

u32* leaf_node_key(void* node, u32 cell_num)
{
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, u32 cell_num)
{
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void print_constants(void)
{
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(u32 level)
{
    char* spaces = xmalloc(level + 1);

    for (u32 i = 0; i != level; ++i) {
        spaces[i] = ' ';
    }

    spaces[level] = '\0';
    printf("%s", spaces);

    xfree(spaces);
}

void print_tree(pager_t* pager, u32 page_num, u32 indentation_level)
{
    void* node;
    u32 num_keys, child, i;

    node = get_page(pager, page_num);
    switch (get_node_type(node)) {
    case (NODE_LEAF):
        num_keys = *leaf_node_num_cells(node);
        indent(indentation_level);

        printf("- leaf (size %d)\n", num_keys);
        for (i = 0; i != num_keys; ++i) {
            indent(indentation_level + 0x1);
            printf("- %d\n", *leaf_node_key(node, i));
        }
        break;
    case (NODE_INTERNAL):
        num_keys = *internal_node_num_keys(node);
        indent(indentation_level);

        // traverse left side of tree
        printf("- internal (size %d)\n", num_keys);
        for (i = 0; i != num_keys; ++i) {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);

            indent(indentation_level + 1);
            printf("- key %d\n", *internal_node_key(node, i));
        }

        // traverse right side of tree
        child = *internal_node_right_child(node);
        print_tree(pager, child, indentation_level + 1);
        break;
    }
}

void leaf_node_insert(cursor_t* c, u32 key, row_t* value)
{
    void* node;
    u32 num_cells, i;

    node = get_page(c->table->pager, c->page_num);
    num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // node is full
        return leaf_node_split_and_insert(c, key, value);
    }

    if (c->cell_num < num_cells) {
        // make room for new cell
        for (i = num_cells; i > c->cell_num; --i) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, c->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, c->cell_num)); // store row
}

node_type_t get_node_type(void* node)
{
    u8 val = *((u8*)(node + NODE_TYPE_OFFSET));
    return (node_type_t)val;
}

void set_node_type(void* node, node_type_t type)
{
    u8 val = type;
    *((u8*)(node + NODE_TYPE_OFFSET)) = val;
}

cursor_t* leaf_node_find(table_t* t, u32 page_num, u32 key)
{
    void* node;
    u32 num_cells, mid, key_at_mid, low, high;
    cursor_t* c;

    node = get_page(t->pager, page_num);
    num_cells = *leaf_node_num_cells(node);

    c = xmalloc(sizeof(cursor_t));
    c->table = t, c->page_num = page_num;

    // binary search with "half-open" interval i.e. [low, high)
    low = 0x0;
    high = num_cells;
    while (high > low) {
        mid = low + ((high - low) / 2);
        key_at_mid = *leaf_node_key(node, mid);

        if (key == key_at_mid) {
            c->cell_num = mid;
            return c;
        }

        if (key > key_at_mid)
            low = mid + 1;
        else
            high = mid;
    }

    c->cell_num = low;
    return c;
}

void leaf_node_split_and_insert(cursor_t* c, u32 key, row_t* value)
{
    void *old_node, *new_node, *dest_node, *dest, *src;
    u32 new_page_num, index_within_node;
    int i;

    /*
        - create a new node and move half of the cells over.
        - insert the new value in one of the 2 nodes
        - update the parent or create a new parent
    */
    old_node = get_page(c->table->pager, c->page_num);
    new_page_num = get_unused_page_num(c->table->pager);
    new_node = get_page(c->table->pager, new_page_num);
    init_leaf_node(new_node);

    /*
        - all existing keys plus new key should be divided evenly
          between old( left ) and new( right ) nodes.
        - Starting from the right, move each key to the correct position
    */
    for (i = LEAF_NODE_MAX_CELLS; i >= 0; --i) {
        dest_node = (i >= LEAF_NODE_LEFT_SPLIT_COUNT) ? new_node : old_node;
        index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        dest = leaf_node_cell(dest_node, index_within_node);

        if (i == c->cell_num) {
            serialize_row(value, dest);
            continue;
        }
        if (i > c->cell_num) {
            src = leaf_node_cell(old_node, i - 1);
            memcpy(dest, src, LEAF_NODE_CELL_SIZE);
            continue;
        }

        src = leaf_node_cell(old_node, i);
        memcpy(dest, src, LEAF_NODE_CELL_SIZE);
    }

    // update cell count on both nodes
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    // update the parent
    if (is_node_root(old_node)) {
        return create_new_root(c->table, new_page_num);
    } else {
        printf("need to implement updating parent after splitting\n");
        exit(EXIT_FAILURE);
    }
}

/*
    Until we start recycling free pages, new pages will be added
    at the end of the db file
*/
u32 get_unused_page_num(pager_t* pager) { return pager->num_pages; }

bool is_node_root(void* node)
{
    u8 value = *((u8*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root)
{
    u8 value = is_root;
    *((u8*)(node + IS_ROOT_OFFSET)) = value;
}

void create_new_root(table_t* t, u32 right_child_page_num)
{
    /*
    Logic of splitting:
        - Let N be the root node. First allocate two nodes, say L and R.
        - Move lower half of N into L and the upper half into R.
        - Now N is empty. Add 〈L, K,R〉 in N, where K is the max key in L.
          Page N remains the root.
        - Note that the depth of the tree has increased by one, but the
          new tree remains height balanced without violating any
          B+-tree property.
    */

    /*
    Algorithm:
        Handle splitting the root
        Old root copied to new page, becomes left child
        Address of right child passed in
        Re-initialize root page to contain the new root node.
        New root node points to two children
    */
    void *root, *left_child, *right_child;
    u32 left_child_page_num, left_child_max_key;

    root = get_page(t->pager, t->root_page_num);
    right_child = get_page(t->pager, right_child_page_num);

    left_child_page_num = get_unused_page_num(t->pager);
    left_child = get_page(t->pager, left_child_page_num);

    // copy all data in root over to left child
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    // root node is now internal with one key and 2 children
    init_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 0x1;
    *internal_node_child(root, 0x0) = left_child_page_num;
    left_child_max_key = get_node_max_key(left_child);
    *internal_node_key(root, 0x0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
}

u32* internal_node_key(void* node, u32 key_num)
{
    return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

u32* internal_node_child(void* node, u32 child_num)
{
    u32 num_keys;

    num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        printf("tried to access a child that's out-of-bounds. child_num %d > "
               "num_keys %d\n",
            child_num, num_keys);
        exit(EXIT_FAILURE);
    }
    if (child_num == num_keys) { // right child
        return internal_node_right_child(node);
    }

    // any other children to the left of the rightmost( left children )
    return internal_node_cell(node, child_num);
}

u32* internal_node_cell(void* node, u32 cell_num)
{
    return node
        + (INTERNAL_NODE_HEADER_SIZE + (cell_num * INTERNAL_NODE_CELL_SIZE));
}

u32* internal_node_right_child(void* node)
{
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

u32* internal_node_num_keys(void* node)
{
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

u32 get_node_max_key(void* node)
{
    u32 key;

    /*
        - for internal nodes, the max key is the rightmost key i.e. the
          "num_keys"-th key
        - for leaf nodes, the max key is the key with the highest
          index i.e. the "num_cells"-th key
    */
    switch (get_node_type(node)) {
    case NODE_INTERNAL:
        key = *internal_node_key(node, *internal_node_num_keys(node) - 0x1);
        break;
    case NODE_LEAF:
        key = *leaf_node_key(node, *leaf_node_num_cells(node) - 0x1);
        break;
    }

    return key;
}

cursor_t* internal_node_find(table_t* t, u32 page_num, u32 key)
{
    cursor_t* c;
    u32 num_keys, min_idx, max_idx, mid, key_to_right, child_num;
    void *node, *child;

    node = get_page(t->pager, page_num);
    num_keys = *internal_node_num_keys(node);

    // cycle thru keys
    // binary search to find index of child to search
    min_idx = 0,
    max_idx = num_keys; // there's one more child than keys

    // todo: try to change it
    while (min_idx != max_idx) {
        mid = min_idx + ((max_idx - min_idx) / 2);
        key_to_right = *internal_node_key(node, mid);

        if (key > key_to_right)
            min_idx = mid + 1;
        else
            max_idx = mid;
    }

    // cycle thru children
    child_num = *internal_node_child(node, min_idx);
    child = get_page(t->pager, child_num);

    switch (get_node_type(child)) {
    case NODE_LEAF:
        c = leaf_node_find(t, child_num, key);
        break;
    case NODE_INTERNAL:
        c = internal_node_find(t, child_num, key);
        break;
    }

    return c;
}

// e n d  o f  B - t r e e

table_t* db_open(const char* fname)
{
    table_t* table;
    pager_t* pager;

    pager = pager_open(fname);
    table = xmalloc(sizeof(table_t));
    table->pager = pager;
    table->root_page_num = 0x0;

    // new db file. initialize page 0 as leaf node
    if (pager->num_pages == 0) {
        void* root_node = get_page(pager, 0x0);
        init_leaf_node(root_node);
        set_node_root(root_node, true);
    }

    return table;
}

void db_close(table_t* t)
{
    pager_t* pager;
    void* curr_page;
    u32 i;
    int result;

    pager = t->pager;
    for (i = 0; i != pager->num_pages; ++i) {
        curr_page = pager->pages[i];
        if (curr_page) {
            pager_flush(pager, i);
            xfree(curr_page);
        }
    }

    result = close(pager->fd);
    if (result < 0) {
        printf("error closing database.\n");
        exit(EXIT_FAILURE);
    }

    xfree(pager), xfree(t);
}

input_buffer_t* new_input_buffer(void)
{
    input_buffer_t* ret;

    ret = xmalloc(sizeof(input_buffer_t));
    ret->buf = NULL, ret->input_len = 0, ret->buf_len = 0;

    return ret;
}

u8 str_exactly_equal(const char* s1, const char* s2)
{
    size_t len1 = strlen(s1);
    size_t len2 = strlen(s2);

    return len1 == len2 && (strncmp(s1, s2, len1) == 0);
}

meta_cmd_result_t exec_meta_cmd(input_buffer_t* in, table_t* t)
{
    if (str_exactly_equal(in->buf, ".exit")) {
        close_input_buffer(in);
        db_close(t);
        exit(EXIT_SUCCESS);
    } else if (str_exactly_equal(in->buf, ".btree")) {
        printf("tree:\n");
        print_tree(t->pager, 0, 0);
        return META_CMD_SUCCESS;
    } else if (str_exactly_equal(in->buf, ".constants")) {
        printf("constants:\n");
        print_constants();
        return META_CMD_SUCCESS;
    }

    return META_CMD_UNRECOGNIZED_CMD;
}

prepare_result_t prepare_insert(input_buffer_t* in, statement* st)
{
    int id;
    char *keyword, *curr_id, *username, *email;

    st->type = STATEMENT_INSERT;

    keyword = strtok(in->buf, " ");
    curr_id = strtok(NULL, " ");
    username = strtok(NULL, " ");
    email = strtok(NULL, " ");
    if (!curr_id || !username || !email) {
        return PREPARE_SYNTAX_ERROR;
    }

    id = atoi(curr_id);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    st->row_to_insert.id = id, strcpy(st->row_to_insert.username, username),
    strcpy(st->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

prepare_result_t prepare_statement(input_buffer_t* in, statement* st)
{
    const char* st_insert = "insert";
    const char* st_select = "select";

    if (!strncmp(st_insert, in->buf, strlen(st_insert))) {
        return prepare_insert(in, st);
    }
    if (!strcmp(st_select, in->buf)) {
        st->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

/*
    Returns a cursor to the leaf node containing the given key.

    If the key is not present, returns a cursor to the leaf node
    where it should be inserted.
*/
cursor_t* table_find(table_t* t, u32 key)
{
    u32 root_page_num;
    void* root_node;

    root_page_num = t->root_page_num;
    root_node = get_page(t->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(t, root_page_num, key);
    }

    return internal_node_find(t, root_page_num, key);
}

execute_result_t exec_insert(statement* st, table_t* t)
{
    void* node;
    row_t* new_row;
    cursor_t* c;
    u32 num_cells, key_to_insert;
    execute_result_t result;

    node = get_page(t->pager, t->root_page_num);
    num_cells = (*leaf_node_num_cells(node));

    new_row = &(st->row_to_insert);
    key_to_insert = new_row->id;

    c = table_find(t, key_to_insert);
    if (c->cell_num < num_cells) {
        u32 key_at_index = *leaf_node_key(node, c->cell_num);
        if (key_at_index == key_to_insert) {
            result = EXECUTE_DUPLICATE_KEY;
            goto cleanup;
        }
    }

    leaf_node_insert(c, new_row->id, new_row);
    result = EXECUTE_SUCCESS;

cleanup:
    xfree(c);
    return result;
}

execute_result_t exec_select(statement* st, table_t* t)
{
    row_t r;
    cursor_t* c;

    for (c = table_start(t); !(c->end_of_table); cursor_advance(c)) {
        deserialize_row(cursor_value(c), &r);
        print_row(&r);
    }

    xfree(c);

    return EXECUTE_SUCCESS;
}

execute_result_t exec_statement(statement* st, table_t* t)
{
    switch (st->type) {
    case STATEMENT_INSERT:
        return exec_insert(st, t);
    case STATEMENT_SELECT:
        return exec_select(st, t);
    }
}

void print_prompt(void) { printf("lyt-db> "); }

int read_input(input_buffer_t* in)
{
    ssize_t bytes_read;

    bytes_read = getline(&in->buf, &in->buf_len, stdin);
    if (bytes_read < 0) {
        return -1;
    }

    in->input_len = bytes_read - 1;

    // remove trailing new-line feed
    in->buf[bytes_read - 1] = 0;

    return 0;
}

void close_input_buffer(input_buffer_t* in) { xfree(in->buf), xfree(in); }

void run_repl(const char* fname)
{
    // todo: deal with all memory leaks
    table_t* table = db_open(fname);
    input_buffer_t* user_input = new_input_buffer();

    // make a REPL
    do {
        print_prompt();
        int ret = read_input(user_input);
        if (ret < 0) {
            return;
        }

        if (!user_input->buf[0]) {
            continue;
        }

        // is it a meta command
        if (user_input->buf[0] == '.') {
            switch (exec_meta_cmd(user_input, table)) {
            case META_CMD_SUCCESS:
                continue;
            case META_CMD_UNRECOGNIZED_CMD:
                PRINT_TO_STDERR("unrecognized command '%s'\n", user_input->buf);
                continue;
            }
        }

        // process the statement
        statement st;
        switch (prepare_statement(user_input, &st)) {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_SYNTAX_ERROR:
            PRINT_TO_STDERR_NOARGS(
                "syntax error. could not parse statement.\n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("unrecognized keyword at start of '%s'.\n", user_input->buf);
            continue;
        case PREPARE_NEGATIVE_ID:
            printf("id must be non-negative.\n");
            continue;
        case PREPARE_STRING_TOO_LONG:
            printf("string is too long.\n");
            continue;
        }

        switch (exec_statement(&st, table)) {
        case EXECUTE_SUCCESS:
            printf("executed.\n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("error: table's full.\n");
            break;
        case EXECUTE_DUPLICATE_KEY:
            printf("error: duplicate key.\n");
            break;
        }
    } while (1);
}

int main(int argc, char* argv[])
{
    const char* fname;

    if (argc != 2) {
        printf("you must supply a database filename.\n");
        printf("usage: %s <db_file>\n", argv[0]);

        return EXIT_FAILURE;
    }

    fname = argv[1];
    run_repl(fname);

    return EXIT_SUCCESS;
}
