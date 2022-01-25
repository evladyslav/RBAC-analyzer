from sys import exit
import numpy as np
from pandas import errors, DataFrame, read_excel
from argparse import ArgumentParser
import warnings


warnings.simplefilter(action='ignore', category=errors.PerformanceWarning)


def parse_args():
    mod = {}
    parsed = []
    parser = ArgumentParser()
parser.add_argum    ent('-p', '--percent', dest='p', type=str, nargs=1)
    parser.add_argument('-i', '--infile', dest='i', nargs=1)
    parser.add_argument('-o', '--outfile', dest='o', nargs=1)
    parser.add_argument('-pc', '--propertycolumn', dest='pc', nargs=1)
    parser.add_argument('-gc', '--groupcolumn', action='append', dest='gc', nargs=2)
    parser.add_argument('-hp', '--hparentcolumn', dest='hp', nargs=1)
    parser.add_argument('-hc', '--hchildcolumn', dest='hc', nargs=1)
    args = parser.parse_args()
    if args.i and args.o and args.p and args.pc[0] and args.gc and args.hp and args.hc:
        parsed.append(args.i)
        parsed.append(args.o)
        mod['p'] = args.p
        mod['pc'] = args.pc[0]
        mod['gc'] = args.gc
        mod['hp'] = args.hp
        mod['hc'] = args.hc
        parsed.append(mod)
        return parsed
    else:
        return None


def sort_by(group, h_parent, h_child, columns):
    sort = []
    check_h = True
    for x in group:
        if x[1] == h_child or x[1] == h_parent:
            if check_h:
                sort = np.append(sort, columns)
                check_h = False
        else:
            sort = np.append(sort, x[1])
    if check_h:
        sort = np.append(sort, columns)
    return sort


def open_datafile(path):
    print(f'Trying to open file - {path}')
    try:
        wb = read_excel(path)
        print(f"OK: File {path} exists")
        return wb
    except IOError:
        print(f"Error: Missing file - {path}")
        exit(0)
        return 0


def make_path(key, tree_dict):
    node = tree_dict.get(key)
    if node is not None:
        upper_path = make_path(node, tree_dict)
    else:
        upper_path = []
    return np.append(upper_path, key)


def drop_col(df, columns):
    df_c = df.copy()
    for column in df_c.columns.tolist():
        if column in columns:
            df_c.drop(column, axis=1, inplace=True)
    return df_c


def convert_(data):
    temp_name = [str(x) for x in data if x != '_']
    st = '<>'.join(temp_name)
    return st


def get_dict_group(df, drop_columns, groups, group_level=0):
    dict_group = {}
    group_level += 1
    group_cut = groups[:group_level]

    for name, group in df.groupby(group_cut.tolist()):
        group1 = group.dropna(how='all', axis='columns').reset_index(drop=True)
        group2 = drop_col(group1, drop_columns).reset_index(drop=True)
        size_df = len(group1)

        def fun2(x):
            x = x.count() / size_df * 100
            if x >= percentage:
                return x
        list_of_properties = group2.apply(fun2).dropna().index.to_list()
        drop_column = np.append(drop_columns, list_of_properties)
        if group_level != len(groups) - 1:
            sub_dict = get_dict_group(group1, drop_column, groups, group_level=group_level)
            dict_group.update(sub_dict)
        if list_of_properties:
            if type(name) == float:
                dict_group[str(int(name))] = list_of_properties
            elif type(name) == tuple:
                new_name = convert_(name)
                dict_group[new_name] = list_of_properties
    return dict_group


if __name__ == '__main__':
    arg = parse_args()
    if arg is None:
        print('Wrong arguments!   Use help "python analyzer.py --help/-h" ')
        exit(0)
    properties_dict = arg[2]
    input_filename = arg[0][0].split("/")[-1]
    output_filename = arg[1][0].split("/")[-1]
    percentage = int(properties_dict['p'][0])
    property_column = properties_dict['pc']
    group_column = properties_dict['gc']
    h_parent_column = properties_dict['hp'][0]
    h_child_column = properties_dict['hc'][0]
    temp_cols = set([x[1] for x in group_column])
    working_cols = {property_column, h_parent_column, h_child_column}.union(temp_cols)
    workbook = open_datafile(input_filename)
    hierarchy = {}
    for index, row in workbook.iterrows():
        hierarchy[row[h_child_column]] = row[h_parent_column]
    paths = []
    ind = []
    for key in hierarchy.keys():
        ind.append(key)
        paths.append(make_path(key, hierarchy))
    df_tree = DataFrame(paths, index=ind)
    df_tree.set_axis([f'level {x}' for x in df_tree.columns.tolist()], axis='columns', inplace=True)
    df_tree.fillna('_', inplace=True)
    for col in workbook.columns.tolist():
        if col not in working_cols:
            workbook.drop(col, axis=1, inplace=True)
    workbook.set_index(h_child_column, inplace=True)
    workbook = workbook.join(df_tree)
    sort_type = sort_by(group_column, h_parent_column, h_child_column, df_tree.columns.tolist())
    workbook.reset_index(drop=True, inplace=True)
    workbook = workbook.assign(pls=np.full(len(workbook), '+')).drop(h_parent_column, axis=1)
    workbook = workbook.pivot_table(index=sort_type.tolist(), columns=property_column, values='pls',
                                    aggfunc=lambda x: 1)
    workbook.sort_values(by=workbook.columns.to_list(), inplace=True)
    workbook.reset_index(inplace=True)
    dc = get_dict_group(workbook, sort_type, sort_type)
    answer = DataFrame.from_dict(get_dict_group(workbook, sort_type, sort_type), orient='index')
    answer.T.to_excel(excel_writer=f'{output_filename}', engine='xlsxwriter')
    print(f'OK. File {output_filename} created.')
