FROM read_csv_auto('{file_path}',ignore_errors=True,delim='|',header=False) src

        LEFT JOIN lkp17 l
            ON TRY_CAST(src.column06 AS VARCHAR) = TRY_CAST(l.TMCODE AS VARCHAR)

        WHERE UPPER(TRY_CAST(src.column07 AS VARCHAR)) != 'D' AND TRY_CAST(src.column10 AS VARCHAR) = '504'
        """)
