import pandas as pd
import os
import sqlite3



col_names=["nr_boczny",
           "przebieg",
           "wozogodziny",
           "awarie",
           "ON",
           "energia",
           "H2"]
prog_path=os.path.dirname(os.path.abspath(__file__))
src_path=prog_path+"\\src\\PR\\"
src_files=os.listdir(src_path)
conn=sqlite3.connect(prog_path+"\\DBus_data.db")
table="operation_PR"



def check_status():
    global  new_src_files, df_new_decom, df_swaps, df_new_swaps

    print("\n\n\t\t      SPRAWDZANIE STATUSU BAZY DANYCH")
    print("___________________________________________________________________________\n")

    #Check db status
    try:
        df_prev_src_files=pd.read_sql_query(f"SELECT src FROM {table}",conn)
        prev_src_files=df_prev_src_files['src'].unique().tolist()

        prev_decom_len=len(pd.read_sql_query("SELECT numer FROM decommission",conn))
        prev_swaps_len=len(pd.read_sql_query("SELECT numer FROM swaps",conn))

        print(f"Ilość rekordów: {len(df_prev_src_files)}\nIlość plików źródłowych: {len(prev_src_files)}\n")

    except pd.io.sql.DatabaseError:
        prev_src_files=[]
        prev_decom_len=0
        prev_swaps_len=0
        
        print("Nie wykryto bazy danych\n")

    #Check source files status        
    new_src_files=[i for i in src_files if i not in prev_src_files]

    if len(new_src_files)>0:
        print("--> Wykryto nowe pliki źródłowe:")
        for file in new_src_files:
            print(file)
        print()
    else:
        print("--> Nie wykryto nowych plików źródłowych")

    #Check modification status
    df_decom=pd.read_excel(prog_path+"\\mod.xlsx",sheet_name="decom",usecols="B,C",index_col=None)
    df_swaps=pd.read_excel(prog_path+"\\mod.xlsx",sheet_name="swaps",usecols="B,C",index_col=None)

    decom_len=len(df_decom.index)
    swaps_len=len(df_swaps.index)

    if decom_len>prev_decom_len:
        print("--> Wykryto nowe kasacje:")
        df_new_decom=df_decom.iloc[prev_decom_len:]
        print(df_new_decom.to_string(index=False))
        print()
    else:
        df_new_decom=pd.DataFrame()
        print("--> Nie wykryto zmian dotyczących kasacji")

    if swaps_len>prev_swaps_len:
        print("--> Wykryto zmiany w numeracji bocznej:")
        df_new_swaps=df_swaps.iloc[prev_swaps_len:]
        print(df_new_swaps.to_string(index=False))
    else:
        df_new_swaps=pd.DataFrame()
        print("--> Nie wykryto zmian w numeracji bocznej")
    


def update_db():
    print("\n\n\t\t         AKTUALIZACJA BAZY DANYCH")
    print("___________________________________________________________________________\n")
    
    #Add data from new source files
    if new_src_files:
        if new_src_files:
            added_records=0
            added_files=0
            df_list=[]
            for file in new_src_files:
                xl_file=pd.ExcelFile(src_path+file)
                xl_sheet=[sheet for sheet in xl_file.sheet_names if (sheet.startswith("POJ")) or (sheet=="KD-3")][0]
                df=pd.read_excel(src_path+file,sheet_name=xl_sheet,usecols="A,C,G,I,T,AB,AC",names=col_names,header=None,skiprows=9,skipfooter=1)       

                #Clean values in "nr boczny" column
                df["nr_boczny"]=df["nr_boczny"].astype(str)
                df["nr_boczny"]=df["nr_boczny"].str.strip("*")
                df["nr_boczny"]=df["nr_boczny"].astype(int)

                #fill NaN values with 0
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

                #Display update status 
                added_records+=len(df.index)
                added_files+=1

                print(f"Dodane rekordy: {added_records}")
                print(f"Dodane pliki: {added_files}\n")
                
            #Combine readed dfs into one
            df_main=pd.concat(df_list,ignore_index=True)

            #Create "id" column as concatenation of "nr boczny" and "src"
            df_main.insert(0,"id",df_main["rok"].astype(str) + "_" + df_main["miesiac"].astype(str)+"_"+df_main["nr_boczny"].astype(str))
                    
            #Add updated table to database
            df_main.to_sql(table,conn,if_exists="append",index=False)


    #Add modifications
    if not df_new_decom.empty or not df_new_swaps.empty:
        df_main=pd.read_sql_query(f"SELECT * FROM {table}",conn)
            
        if not df_new_decom.empty:
            df_decom_swaps=df_new_decom.merge(df_swaps,on="numer")
                
            if not df_decom_swaps.empty:
                decom_swaps_dict=df_decom_swaps.set_index("stary_numer").T.to_dict("list")
                new_swaps_dict=dict(zip(df_new_swaps.stary_numer,df_new_swaps.numer))
            
                for key in new_swaps_dict:
                    #Replace "nr boczny" of decommissioned bus if it is/was recognized as swapped (even previously replaced to prevent possible errors)
                    if key in decom_swaps_dict:
                        df_main.nr_boczny.replace(new_swaps_dict[key],key,inplace=True) #Undo previous swap
                        df_main.loc[(df_main["nr_boczny"]==key) & ((df_main["rok"]<int(str(decom_swaps_dict[key][1])[:4])) | ((df_main["rok"]==int(str(decom_swaps_dict[key][1])[:4]) & (df_main["miesiac"]<int(str(decom_swaps_dict[key][1])[5:7]))))),"nr_boczny"]=new_swaps_dict[key]
                    #Replace "nr boczny" of non decommissioned buses
                    else:
                        df_main.nr_boczny.replace(key,new_swaps_dict[key],inplace=True)
                            
            elif not df_new_swaps.empty:
                new_swaps_dict=dict(zip(df_new_swaps.stary_numer,df_new_swaps.numer))
                
                for key in new_swaps_dict:
                    df_main.nr_boczny.replace(key,new_swaps_dict[key],inplace=True)
                    
            #Add decommission tag
            new_decom_dict=dict(zip(df_new_decom.numer,df_new_decom.kasacja))
            for key in new_decom_dict:
                df_main.loc[(df_main["nr_boczny"]==key) & ((df_main["rok"]<int(str(new_decom_dict[key])[:4])) | ((df_main["rok"]==int(str(new_decom_dict[key])[:4])) & ((df_main["miesiac"]<int(str(new_decom_dict[key])[5:7]))))),"nr_boczny"]=df_main["nr_boczny"].astype(str)+"_D"+str(new_decom_dict[key])[2:4]
                    
        #Replace "nr boczny" if there are no decommission modifications
        elif not df_new_swaps.empty:
            new_swaps_dict=dict(zip(df_new_swaps.stary_numer,df_new_swaps.numer))
            for key in new_swaps_dict:
                df_main.nr_boczny.replace(key,new_swaps_dict[key],inplace=True)
    
        #Update "id" column as concatenation of "nr boczny" and "src"
        df_main["id"]=df_main["rok"].astype(str)+"_"+df_main["miesiac"].astype(str)+"_"+df_main["nr_boczny"].astype(str)

        #Add updated tables to db
        df_main.to_sql(table,conn,if_exists="replace",index=False)
        df_new_decom.to_sql("decommission",conn,if_exists="append",index=False)
        df_new_swaps.to_sql("swaps",conn,if_exists="append",index=False)

        print(f"\nPomyślnie przeprowadzono modyfikacje")



if __name__=="__main__":
    print('\t\t\t\t  "DBus"\n')
    print('''Program służący do tworzenia bazy danych zawierającej zagregowane informacje
dotyczące eksploatacji taboru autobusowego. Źródłem danych są pliki Excel,
przekazywane co miesiąc przez dział PR.''')

    check_status()
    if new_src_files or not df_new_swaps.empty or not df_new_decom.empty:
        update=input("\n\nAby przeprowadzić aktualizację bazy danych - wprowadź 1\nAby zakończyć - naciśnij ENTER\n")
        if update=="1":
            update_db()
            input("\n\nBaza danych jest aktualna\nAby zakończyć - naciśnij ENTER\n")
    else:
        input("\nBaza danych jest aktualna\nAby zakończyć - naciśnij ENTER\n")
