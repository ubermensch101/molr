def add_akarbandh(psql_conn, input_table, akarbandh_table, akarbandh_col):        
    sql = f'''
        alter table {input_table}
        drop column if exists {akarbandh_col};

        alter table {input_table}
        add column {akarbandh_col} float;
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)
        
    sql = f'''
        update {input_table} as p
        set {akarbandh_col} = (select area from {akarbandh_table} where survey_no = p.survey_no)
    '''
    with psql_conn.connection().cursor() as curr:
        curr.execute(sql)