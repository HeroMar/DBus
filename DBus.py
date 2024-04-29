import pandas as pd
import numpy as np
import os
import sqlite3


col_names=["nr_boczny",
           "przebieg",
           "wozogodziny",
           "awarie",
           "ON",
           "energia"]

prog_path=os.path.dirname(os.path.abspath(__file__))
src_path=prog_path+"\\src\\PR\\"
src_files=os.listdir(src_path)

conn=sqlite3.connect(prog_path+"\\busdata.db")


def check_status():
    global  new_src_files, df_new_swaps

    print("\n\n\t\t      SPRAWDZANIE STATUSU BAZY DANYCH")
    print("___________________________________________________________________________\n")
   
    #Check db status
    try:
        df_prev_src_files=pd.read_sql_query("SELECT src FROM main",conn)
        prev_src_files=df_prev_src_files['src'].unique().tolist()

        prev_swaps_len=len(pd.read_sql_query("SELECT stary FROM swaps",conn))

        print(f"- ilość rekordów: {len(df_prev_src_files)}\n- ilość plików źródłowych: {len(prev_src_files)}")

    except pd.io.sql.DatabaseError:
        prev_src_files=[]
        prev_swaps_len=0

        print("Nie wykryto bazy danych")

    #Check source files status
    print("\n\nPliki źródłowe:")
        
    new_src_files=[i for i in src_files if i not in prev_src_files]

    if len(new_src_files)>0:
        print("Wykryto nowe pliki źródłowe:")
        for file in new_src_files:
            print(file)
    else:
        print("Nie wykryto nowych plików źródłowych - baza danych jest aktualna")

    #Check swaps status
    print("\n\nNr boczne:")
    
    df_swaps=pd.read_excel(prog_path+"\\swaps.xlsx",usecols="B,C,D,E",index_col=None)
    swaps_len=len(df_swaps.index)

    if swaps_len>prev_swaps_len:
        print("Wykryto zmiany dla następujących nr bocznych autobusów:")
        df_new_swaps=df_swaps.iloc[prev_swaps_len:]
        print(df_new_swaps.iloc[:,0:2].to_string(index=False))
    else:
        df_new_swaps=pd.DataFrame()
        print("Nie wykyrto zmian - nr boczne autobusów są aktualne")


def update_db():
    print("\n\n\t\t         AKTUALIZACJA BAZY DANYCH")
    print("___________________________________________________________________________\n")

    if len(new_src_files)>0 or len(df_new_swaps.index)>0:
        if len(new_src_files)>0:
            added_records=0
            added_files=0
            df_list=[]
            for file in new_src_files:
                xl_file=pd.ExcelFile(src_path+file)
                xl_sheet=[sheet for sheet in xl_file.sheet_names if (sheet.startswith("POJ")) or (sheet=="KD-3")][0]
                df=pd.read_excel(src_path+file,sheet_name=xl_sheet,usecols="A,C,G,I,T,AB",names=col_names,header=None,skiprows=9,skipfooter=1)       

                #Clean values in "nr boczny" column
                df["nr_boczny"]=df["nr_boczny"].astype(str)
                df["nr_boczny"]=df["nr_boczny"].str.strip("*")
                df["nr_boczny"]=df["nr_boczny"].astype(int)

                #Fill NaN values with 0
                df.fillna(0,inplace=True)

                #Create "src" column
                df["src"]=file

                #Create "rok" column
                df.insert(1,"rok","20"+df["src"].str[4:6])
                df["rok"]=df["rok"].astype(int)

                #Create "miesiac" column
                df.insert(2,"miesiac",df["src"].str[2:4])
                df["miesiac"]=df["miesiac"].astype(int)

                #Add single df to list of dfs
                df_list.append(df)

                #Show update status 
                added_records+=len(df.index)
                added_files+=1
                print(f"Dodane rekordy: {added_records}")
                print(f"Dodane pliki: {added_files}\n")
                
            #Concatenation of readed dfs
            df_main=pd.concat(df_list,ignore_index=True)

            #Create "id" column as concatenation of "nr boczny" and "src"
            df_main.insert(0,"id",df_main["rok"].astype(str) + "_" + df_main["miesiac"].astype(str)+"_"+df_main["nr_boczny"].astype(str))
                    
            #Add updated table to database
            df_main.to_sql("main",conn,if_exists="append",index=False)

        else:
            print(f"Brak nowych plików źródłowych\n")

        if len(df_new_swaps.index)>0:
            df_main=pd.read_sql_query("SELECT * FROM main",conn)
 
            new_swaps_list=df_new_swaps.values.tolist()
            for record in new_swaps_list:
                df_main['nr_boczny']=np.where((df_main['nr_boczny']==record[0]) & (df_main['miesiac']<=record[2]) & (df_main['rok']<=record[3]),record[1],df_main['nr_boczny'])
                
            #Update "id" column as concatenation of "nr boczny" and "src"
            df_main["id"]=df_main["rok"].astype(str) + "_" + df_main["miesiac"].astype(str)+"_"+df_main["nr_boczny"].astype(str)

            #Add updated tables to db
            df_main.to_sql("main",conn,if_exists="replace",index=False)
            df_new_swaps.to_sql("swaps",conn,if_exists="append",index=False)

            print(f"\nZaktualizowano nr boczne autobusów - ilość zmienionych nr bocznych: {len(df_new_swaps.index)}")
    
        else:
            print(f"Nie wykryto nowych zmian w nr bocznych autobusów\n")

    else:
        print("Nie wykryto nowych zmian do wprowadzenia")



if __name__=="__main__":
    print('\t\t     BAZA DANYCH "AUTOBUSY PR" ver. 1.0        07.12.2023 r.\n')

    print('''Program służący do tworzenia bazy danych na podstawie comiesięcznych plików
przekazywanych przez dział PR.''')

    check_status()
    update=input("\n\n\nAby przeprowadzić aktualizację bazy danych - wprowadź 1\nAby zakończyć - naciśnij ENTER.\n")
    if update=="1":
        update_db()
        input("\n\nAby zakończyć - naciśnij ENTER\n")
    else:
        pass

    conn.close()
