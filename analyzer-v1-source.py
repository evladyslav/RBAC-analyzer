from sys import exit
from pandas import read_excel, errors, DataFrame
from argparse import ArgumentParser
import warnings
from numpy import append, full

warnings.simplefilter(action='ignore', category=errors.PerformanceWarning)


def parse_args():
    mod = {}
    parsed = []
    parser = ArgumentParser()
    parser.add_argument('-p', '--percent', dest='p', type=str, nargs=1)
    parser.add_argument('-i', '--infile', dest='i', nargs=1)
    parser.add_argument('-o', '--outfile', dest='o', nargs=1)
    parser.add_argument('-pc', '--propertycolumn', dest='pc', nargs=1)
    parser.add_argument('-gc', '--groupcolumn', action='append', dest='gc', nargs=2)
    parser.add_argument('-hp', '--hparentcolumn', dest='hp', nargs=1)
    parser.add_argument('-hc', '--hchildcolumn', dest='hc', nargs=1)
    args = parser.parse_args()

    if args:
        mod['p'] = args.p
        parsed.append(args.i)
        parsed.append(args.o)
        mod['pc'] = args.pc[0]
        mod['gc'] = args.gc
        mod['hp'] = args.hp
        mod['hc'] = args.hc
        parsed.append(mod)
    return parsed


def sort_by(group, h_parent, h_child, columns):
    sort = []
    check_h = True
    for x in group:
        if x[1] == h_child or x[1] == h_parent:
            if check_h:
                sort = append(sort, columns)
                check_h = False
        else:
            sort = append(sort, x[1])
    if check_h:
        sort = append(sort, columns)
    return sort


def open_datafile(path):
    try:
        wb = read_excel(path)
        print(f"OK: File {path} exists")
        return wb
    except IOError:
        print(f"Error: Missing file - {path}")
        return 0


def make_path(key, tree_dict):
    node = tree_dict.get(key)
    if node is not None:
        upper_path = make_path(node, tree_dict)
    else:
        upper_path = []
    return append(upper_path, key)


if __name__ == '__main__':
    arg = parse_args()
    properties_dict = arg[2]
    input_filename = arg[0][0].split("/")[-1]
    output_filename = arg[1][0].split("/")[-1]
    percentage = properties_dict['p']
    property_column = properties_dict['pc']
    group_column = properties_dict['gc']
    h_parent_column = properties_dict['hp']
    h_child_column = properties_dict['hc']
    temp_cols = set([x[1] for x in group_column])
    working_cols = {property_column, h_parent_column[0], h_child_column[0]}.union(temp_cols)
    workbook = open_datafile(input_filename)

    if not arg:
        print('Wrong arguments.. ')
        exit(0)
    h_columns = [h_parent_column[0], h_child_column[0]]
    tree = {}
    for index, row in workbook.iterrows():
        tree[row[h_child_column[0]]] = row[h_parent_column[0]]
    paths = []
    ind = []
    for x in tree.keys():
        ind.append(x)
        paths.append(make_path(x, tree))
    df_tree = DataFrame(paths, index=ind)
    df_tree.set_axis([f'level {x}' for x in df_tree.columns.tolist()], axis='columns', inplace=True)
    df_tree.fillna('_', inplace=True)
    for col in workbook.columns.tolist():
        if col not in working_cols:
            workbook.drop(col, axis=1, inplace=True)
    workbook.set_index(h_child_column[0], inplace=True)
    workbook = workbook.join(df_tree)
    sort_type = sort_by(group_column, h_parent_column[0], h_child_column[0], df_tree.columns.tolist())
    workbook = workbook.assign(pls=full(len(workbook), '+')).drop(h_parent_column[0], axis=1)
    workbook = workbook.pivot_table(index=sort_type.tolist(), columns=property_column, values='pls',
                                    aggfunc=lambda x: '+')
    workbook.T.sort_values(by=workbook.columns.to_list(), inplace=True)
    workbook.to_excel(excel_writer=f'{output_filename}', engine='xlsxwriter')
    print(f'OK. File{output_filename} created')