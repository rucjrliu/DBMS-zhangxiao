//
// Created by N on 2021/10/23.
//
#include <regex>
#include <iostream>
#include <string>
#include <algorithm>
#include "io.h"
#include <cstdio>
#include <cwchar>
#include <cstdlib>

#include "map"
#include "hash_map"
#include "hash_map_hpp.h"

#define NON 0
#define PK 1
#define NOT_NULL 2
#define UNIQUE 3
#define BUFFER_SIZE 50
#define PAGE_SIZE 8192

using namespace std;

class Table;

class Page {
public:
    Table *table = nullptr;
    int used_items_num = 0;
    Page *pre_page = nullptr;
    Page *next_page = nullptr;
    char *page = nullptr;
    char *writer = nullptr;   // ptr pointing to the unused addr in the page if not full else null ptr
    bool is_dirty = false;

    ~Page();

    void initialize(Table *table);

    bool is_init();

    void *get_item(int item_idx);

    void write_item(char *item);    //todo


};

void Page::initialize(Table *t) {
    table = t;
    pre_page = nullptr;
    next_page = nullptr;
    page = (char *) malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    writer = page;
}

bool Page::is_init() {
    return table != nullptr;
}

Page::~Page() {
    if (page != nullptr) free(page);
}

class Buffer {
public:

    Page *head2load = new Page();    // link list without a head
    Page *head2unload = new Page();

    map<Table *, Page *> pages_in_use;

    Buffer();

    void load_new_page2in_use(Table *table);

    Page *get_new_page(Table *table);

    void swap_out();

    void unload_a_page();

    void schedule_page(Page *obj_page);   // waiting for the index module

    void insert2ll_tail(Page *obj_page);

    void _insert2head_of_load_ll(Page *obj_page);

    void _insert2head_of_unload_ll(Page *obj_page);

    void _insert2head_of_ll(Page *obj_page, Page *head);

    void _write_page(Page *page);
};

Buffer::Buffer() {
    Page *page;
    for (int i = 0; i < BUFFER_SIZE; ++i) {
        page = new Page();
        _insert2head_of_load_ll(page);
    }
}

void Buffer::load_new_page2in_use(Table *table) {
    Page *page2load = head2load->pre_page;
    if (page2load != head2load) {
        head2load->pre_page = page2load->pre_page;
        page2load->pre_page->next_page = head2load;
        page2load->initialize(table);
        pages_in_use[table] = page2load;
    } else {
        swap_out();
        page2load = new Page();
        page2load->initialize(table);
        pages_in_use[table] = page2load;
    }
}

void Buffer::_insert2head_of_load_ll(Page *obj_page) {
    _insert2head_of_ll(obj_page, head2load);
}

void Buffer::_insert2head_of_unload_ll(Page *obj_page) {
    _insert2head_of_ll(obj_page, head2unload);
}

void Buffer::_insert2head_of_ll(Page *obj_page, Page *head) {
    Page *orig_first = head->next_page;
    if (orig_first != nullptr) {
        obj_page->next_page = orig_first;
        orig_first->pre_page = obj_page;
    } else {
        obj_page->next_page = head;
        head->pre_page = obj_page;
    }
    obj_page->pre_page = head;
    head->next_page = obj_page;
}


Page *Buffer::get_new_page(Table *table) {
    Page *page2load = head2load->pre_page;
    if (page2load != head2load) {
        head2load->pre_page = page2load->pre_page;
        page2load->pre_page->next_page = head2load;
        page2load->initialize(table);
    } else {
        swap_out();
        page2load = new Page();
        page2load->initialize(table);
    }
    _insert2head_of_unload_ll(page2load);
    return page2load;
}

Buffer *buffer = new Buffer();

class Table {
public:
    int item_len = 0;     // item means a record in the table
    char *page_buffer = new char[PAGE_SIZE];
    char *buffer_reader = nullptr;
    FILE *fp_data = nullptr;
    FILE *fp_meta = nullptr;
    int capacity_of_items_per_page;
    int num_pages;
    int pk_len; // todo
    int num_items;
    vector<pair<int, int>> to_del;
    string table_name;
    __gnu_cxx::hash_map<int, Page *> idx2page;
    map<Page *, int> page2idx;
    __gnu_cxx::hash_map<string, pair<int, int>> item_dict;  // {primary_key:(page*,idx_of_page,idx_in_page),...}
    static vector<string> types;
    static vector<string> constraints;
    static regex redundant;
    vector<string> attributes;
    __gnu_cxx::hash_map<string, tuple<string, int, int, int>> attr_dict;   // {var_name:(type,[constraint_type,capacity,offset_in_item])}
    __gnu_cxx::hash_map<string, __gnu_cxx::hash_map<string, vector<string>>> lut4idx_tables;

    Table(string table_name, vector<vector<string>> vars_with_constraints);

    ~Table();

    template<typename T>
    T get_value();

    void add_item(char *item);

    void del_item_with_pk(string pk);

    void create_idx(string var_name);

    string get_attr(string item, string var_name);

    void load_meta();

    // 4 doing test in hw1
    void read_tbl(string tbl_file_path);

    void write_seg_in_mem();

    string lu_item_with_pk(string pk);

    void lu_item_with_idx_key(string idx_var_name, string key);

};

vector<string> Table::types = {"varchar", "char", "int", "long", "float", "double", "date"};
vector<string> Table::constraints = {"", "primary", "null", "unique"};
regex Table::redundant("\\0+$");

Table::Table(string table_name, vector<vector<string>> vars_with_constraints) : table_name(table_name) {
    this->table_name = table_name;
    int offset = 0;
    for (const auto &item: vars_with_constraints) {
        string name = item[0];
        string type = item[1];
        string orig_type = type;
        int capacity = 0;
        string constraint = item[2];
        int constraint_type = 0;
        for (const auto &item: types) {
            if (type.find(item) != type.npos) {
                type = item;
                break;
            }
        }
        if (strcmp(type.c_str(), "varchar") == 0 || strcmp(type.c_str(), "char") == 0) {
            capacity = atoi(orig_type.substr(orig_type.find("(") + 1, orig_type.find(")") - 1).c_str());
        } else if (strcmp(type.c_str(), "int") == 0 || strcmp(type.c_str(), "float") == 0) capacity = 4;
        else if (strcmp(type.c_str(), "long") == 0 || strcmp(type.c_str(), "double") == 0 ||
                 strcmp(type.c_str(), "date") == 0)
            capacity = 8;
        else cerr << table_name << " definition has a syntax error with attr type";
        for (int i = 1; i < constraints.size(); ++i) {
            if (constraint.find(constraints[i]) != constraint.npos) {
                constraint_type = i;
                break;
            }
        }
        // {var_name:(type,constraint_type,capacity),...}
        attr_dict[name] = tuple<string, int, int, int>(type, constraint_type, capacity, offset);
        offset += capacity;
        attributes.emplace_back(name);
    }
    for (const auto &var: attributes) item_len += get<2>(attr_dict[var]);
    capacity_of_items_per_page = PAGE_SIZE / item_len;
    string data_path = table_name + ".tb";
    string meta_path = table_name + "_meta.tb";
    if (access(data_path.c_str(), 0) == -1) {
        num_pages = 0;
        pk_len = 0;
        num_items = 0;
        fp_data = fopen(data_path.c_str(), "wb+");
        fp_meta = fopen(meta_path.c_str(), "wb+");
        return;
    }
    fp_data = fopen(data_path.c_str(), "rb+");
    fp_meta = fopen(meta_path.c_str(), "rb+");
    load_meta();
}

template<typename T>
T Table::get_value() {
    T *p = reinterpret_cast<T *>(buffer_reader);
    T ret = *p++;
    buffer_reader = reinterpret_cast<char *>(p);
    return ret;
}

void Table::add_item(char *item) {
    if (buffer->pages_in_use.find(this) == buffer->pages_in_use.end()) {
        buffer->load_new_page2in_use(this);
        idx2page[num_pages] = buffer->pages_in_use[this];
        page2idx[buffer->pages_in_use[this]] = num_pages;
        num_pages++;
    }
    Page *page = buffer->pages_in_use[this];
    memcpy(page->writer, item, item_len);
    page->is_dirty = true;
    page->writer += item_len;
    string temp_pk = to_string(num_items++);
    pk_len = temp_pk.size();
    item_dict[temp_pk] = {page2idx[page], page->used_items_num++};
    if (page->used_items_num == capacity_of_items_per_page) {
        buffer->_insert2head_of_unload_ll(page);
        buffer->pages_in_use.erase(this);
    }
}

void Table::load_meta() {
    fseek(fp_meta, 0, SEEK_END);
    int len = ftell(fp_meta);
    if (len == 0) {
        num_pages = 0;
        pk_len = 0;
        num_items = 0;
        return;
    }
    char *buf_scratch = len > PAGE_SIZE ? new char[len] : page_buffer;
    fseek(fp_meta, 0, SEEK_SET);

    buffer_reader = buf_scratch;
    num_pages = get_value<int>();
    pk_len = get_value<int>();
    num_items = get_value<int>();
    int idx_of_page, idx_in_page;
    string pk;
    for (int i = 0; i < num_items; ++i) {
        pk = string(buffer_reader);
        buffer_reader += pk_len;
        idx_of_page = get_value<int>();
        idx_in_page = get_value<int>();
        item_dict[pk] = {idx_of_page, idx_in_page};
    }
    // todo wait the implement of index module and del temp_pk
}

string Table::lu_item_with_pk(string pk) {
    if (idx2page.find(item_dict[pk].first) == idx2page.end()) {
        fseek(fp_data, item_dict[pk].first * PAGE_SIZE, SEEK_SET);
        fread(page_buffer, 1, PAGE_SIZE, fp_data);
        // method get_new_page will schedule the page to the head of unload linklist
        Page *page = buffer->get_new_page(this);
        memcpy(page->page, page_buffer, PAGE_SIZE);
        idx2page[item_dict[pk].first] = page;
        page2idx[page] = item_dict[pk].first;
        return string(page->page + item_dict[pk].second * item_len, item_len);
    } else {
        auto p = idx2page[item_dict[pk].first]->page;
        auto offset = item_dict[pk].second * item_len;
        return string(p + offset, item_len);
    }
}

void Table::del_item_with_pk(string pk) {
    // to mark instead of del
    if (item_dict.find(pk) == item_dict.end()) cout << "not such a item with pk = " << pk << endl;
    else to_del.emplace_back(item_dict[pk]);
}

void Table::write_seg_in_mem() {
    fseek(fp_meta, 0, SEEK_SET);
    fwrite(&num_pages, 1, sizeof(int), fp_meta);
    fwrite(&pk_len, 1, sizeof(int), fp_meta);
    fwrite(&num_items, 1, sizeof(int), fp_meta);
    char *buf = new char[pk_len];
    for (int i = 0; i < num_items; ++i) {
        string pk = to_string(i);
        memset(buf, 0, pk_len);
        memcpy(buf, pk.c_str(), pk.size());
        fwrite(buf, 1, pk_len, fp_meta);
        // idx of page
        fwrite(&item_dict[pk].first, 1, sizeof(int), fp_meta);
        // idx in page
        fwrite(&item_dict[pk].second, 1, sizeof(int), fp_meta);
    }
    for (int i = 0; i < num_pages; ++i) {
        fseek(fp_data, i * PAGE_SIZE, SEEK_SET);
        fwrite(idx2page[i]->page, 1, PAGE_SIZE, fp_data);
        delete idx2page[i];
        buffer->_insert2head_of_load_ll(new Page());
    }
}

Table::~Table() {
    write_seg_in_mem();
    fclose(fp_meta);
    fclose(fp_data);
}

void Table::read_tbl(string tbl_file_path) {
    char *buf = new char[2 * PAGE_SIZE];
    FILE *f = fopen(tbl_file_path.c_str(), "r");
    fseek(f, 0, SEEK_END);
    int len = ftell(f);
    for (int offset = 0; offset < len;) {
        fseek(f, offset, SEEK_SET);
        memset(buf, 0, 2 * PAGE_SIZE);
        fread(buf, 1, 2 * PAGE_SIZE, f);
        string buf_str = string(buf);
        regex end_line("\n");
        regex sep("\\|");
        vector<string> lines(sregex_token_iterator(buf_str.begin(), buf_str.end(), end_line, -1),
                             sregex_token_iterator());
        for (int i = 0; i < capacity_of_items_per_page && offset < len; ++i) {
            vector<string> attrs(sregex_token_iterator(lines[i].begin(), lines[i].end(), sep, -1),
                                 sregex_token_iterator());
            char *item = new char[item_len];
            char *item_writer = item;
            for (int j = 0; j < attributes.size(); ++j) {
                string attr_name = attributes[j];
                if (strcmp(get<0>(attr_dict[attr_name]).c_str(), "int") == 0) {
                    int v = atoi(attrs[j].c_str());
                    memcpy(item_writer, &v, sizeof(int));
                    item_writer += sizeof(int);
                } else if (strcmp(get<0>(attr_dict[attr_name]).c_str(), "long") == 0) {
                    long long v = atoll(attrs[j].c_str());
                    memcpy(item_writer, &v, sizeof(long long));
                    item_writer += sizeof(long long);
                } else if (strcmp(get<0>(attr_dict[attr_name]).c_str(), "float") == 0) {
                    float v = (float) atof(attrs[j].c_str());
                    memcpy(item_writer, &v, sizeof(float));
                    item_writer += sizeof(float);
                } else if (strcmp(get<0>(attr_dict[attr_name]).c_str(), "double") == 0) {
                    double v = atoi(attrs[j].c_str());
                    memcpy(item_writer, &v, sizeof(double));
                    item_writer += sizeof(double);
                } else {
                    memcpy(item_writer, attrs[j].c_str(), attrs[j].size());
                    item_writer += get<2>(attr_dict[attr_name]);
                }
                offset += attrs[j].size();
            }
            offset += attributes.size() + 1;
//            fseek(f,offset,SEEK_SET);
//            fread(buf,1,2*PAGE_SIZE,f);
            add_item(item);
            delete item;
            cout << "loading " << table_name << " item with idx: " << num_items << endl;
        }
    }
}

void Table::create_idx(string var_name) {
    lut4idx_tables[var_name] = __gnu_cxx::hash_map<string, vector<string>>();
    for (const auto &item: item_dict) {
        string item_str = lu_item_with_pk(item.first);
        string attr = get_attr(item_str, var_name);
        if (lut4idx_tables[var_name].find(attr) == lut4idx_tables[var_name].end())
            lut4idx_tables[var_name][attr] = vector<string>{item.first};
        else lut4idx_tables[var_name][attr].emplace_back(item.first);
    }
}

string Table::get_attr(string item, string var_name) {
    string type = get<0>(attr_dict[var_name]);
    buffer_reader = (char *) item.c_str();
    buffer_reader += get<3>(attr_dict[var_name]);
    if (strcmp(type.c_str(), "int") == 0) return to_string(get_value<int>());
    else if (strcmp(type.c_str(), "long") == 0) return to_string(get_value<long long>());
    else if (strcmp(type.c_str(), "float") == 0) return to_string(get_value<float>());
    else if (strcmp(type.c_str(), "double") == 0) return to_string(get_value<double>());
    else return regex_replace(string(buffer_reader, get<2>(attr_dict[var_name])), redundant, "");
}

void Table::lu_item_with_idx_key(string idx_var_name, string key) {
    if (lut4idx_tables.find(idx_var_name) == lut4idx_tables.end()){} // to lu without idx
    else if (lut4idx_tables[idx_var_name].find(key) == lut4idx_tables[idx_var_name].end())
        cout << "not such item with " << idx_var_name << " of " << key << endl;
    else
        for (const auto &pk: lut4idx_tables[idx_var_name][key])
            cout << lu_item_with_pk(pk);
}

vector<Table *> table_list;

void Buffer::swap_out() {
    // take last recently used page from the tail
    Page *page = head2unload->pre_page;
    page->pre_page->next_page = head2unload;
    head2unload->pre_page = page->pre_page;
    cout << "last recently used page with idx:" << page->table->page2idx[page] << " has been swap out";
    if (page->is_dirty) {
        int idx = page->table->page2idx[page];
        fseek(page->table->fp_data, idx * PAGE_SIZE, SEEK_SET);
        fwrite(page->page, 1, PAGE_SIZE, page->table->fp_data);
        cout << " with writing back to file" << endl;
    } else cout << endl;
    page->table->idx2page.erase(page->table->page2idx[page]);
    page->table->page2idx.erase(page);
    delete page;
}

class SQLParser {
public:
    static const string pattern4drop;
    static const string pattern4create;
    static const string pattern4vars;
    static const string pattern_of_table_or_var_name;
    static const string pattern_of_var_type;
    static const string pattern_of_constraint4att;


    static void parse_SQL(string obj_str);

    static vector<vector<string>> catch_variables(string obj_str);

    static void split(const string &s, vector<string> &v, const string &c);
};

class utf8_char;

class utf8_char;

const string SQLParser::pattern_of_table_or_var_name = "([^ ]*?)";
const string SQLParser::pattern_of_var_type = "(varchar\\(\\d*?\\)|char\\(\\d*?\\)|integer|long|float|double|date)";
// todo: check, constraint c1, constraint 4 relation
const string SQLParser::pattern_of_constraint4att = " *?(not +?null|unique|primary +?key|)";
const string SQLParser::pattern4create = "create +?table +?" + pattern_of_table_or_var_name + " *?\\((.*+)\\)";
const string SQLParser::pattern4drop = "drop +?table +?" + pattern_of_table_or_var_name;
const string SQLParser::pattern4vars =
        " *?" + pattern_of_table_or_var_name + " +?" + pattern_of_var_type + pattern_of_constraint4att;

void SQLParser::split(const string &s, vector<string> &v, const string &c) {
    string::size_type pos1, pos2;
    size_t len = s.length();
    pos2 = s.find(c);
    pos1 = 0;
    while (string::npos != pos2) {
        v.emplace_back(s.substr(pos1, pos2 - pos1));

        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != len)
        v.emplace_back(s.substr(pos1));
}

void SQLParser::parse_SQL(string obj_str) {
    transform(obj_str.begin(), obj_str.end(), obj_str.begin(), ::tolower);
    regex pat4create(pattern4create);
    regex pat4drop(pattern4drop);
    smatch m;
    if (regex_match(obj_str, m, pat4create)) {
        string table_name = m[1];
        string t = m[2];
        vector<vector<string>> vars_with_constraints = catch_variables(t);
        Table *table = new Table(table_name, vars_with_constraints);
        table_list.emplace_back(table);
    } else if (regex_match(obj_str, m, pat4drop)) {
        cout << "dropping table is on the way" << endl;
    } else {
        cerr << "syntax error";
        exit(1);
    }
}


vector<vector<string>> SQLParser::catch_variables(string obj_str) {
    vector<string> var_defs;
    vector<vector<string>> vars_with_constraints;
    split(obj_str, var_defs, ",");
    for (const auto &def: var_defs) {
        vector<string> t;
        regex r(pattern4vars);
        smatch m;
        if (!regex_match(def, m, r)) {

            cerr << "syntax error";
            exit(1);
        }
        for (int i = 1; i <= 3; ++i) t.emplace_back(m[i]);
        vars_with_constraints.emplace_back(t);
    }
    return vars_with_constraints;
};

int main() {
    fclose(fopen("supplier_meta.tb", "w"));
    fclose(fopen("supplier.tb", "w"));
    iostream::sync_with_stdio(true);
    string s = "create table supplier ( s_suppkey integer not null, s_name char(25) not null, s_address varchar(40) not null, s_nationkey integer not null,s_phone char(15) not null, s_acctbal float not null, s_comment varchar(101) not null)";
    SQLParser::parse_SQL(s);
    string tbl_file_path = "supplier.tbl";
    table_list[0]->read_tbl(tbl_file_path);
    table_list[0]->create_idx("s_name");
//    table_list[0]->create_idx("s_address");
    table_list[0]->create_idx("s_suppkey");
//    for (const auto &var_name: {"s_suppkey"}) {
//        for (const auto &item: table_list[0]->lut4idx_tables[var_name]) {
//            cout << "key: " << item.first << ", values: ";
//
//            for (const auto &item: item.second)
//                cout << item << " ";
//            cout << endl;
//        }
//        cout << "------------------" << endl;
//        auto iter=table_list[0]->lut4idx_tables[var_name].begin();
//        while (iter!=table_list[0]->lut4idx_tables[var_name].end()){
//            cout<<"key: "<<iter->first<<", values: ";
//            for (const auto &item : iter->second)
//                cout<<item<<" ";
//            cout<<endl;
//            iter= next(iter);
//        }

//
//    }
//    table_list[0]->lu_item_with_idx_key("s_name", "Supplier#000000059");
//    table_list[0]->lu_item_with_idx_key("s_address", "n48Wy4QI3lml8T217rk");
//    cout<<table_list[0]->lu_item_with_pk(to_string(849))<<endl;
//    cout<<table_list[0]->lu_item_with_pk(to_string(84))<<endl;
    cout<<table_list[0]->lu_item_with_pk(to_string(1849))<<endl;
    //todo: waiting for io is collided with the program?
    table_list[0]->lu_item_with_idx_key("s_suppkey", "85");
    table_list[0]->lu_item_with_idx_key("s_suppkey", "850");
//    table_list[0]->lu_item_with_idx_key("s_suppkey", "1850");
//    delete table_list[0];
    system("pause");
}
