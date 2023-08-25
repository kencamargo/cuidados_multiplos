#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include "sqlite3.h"

using namespace std;

vector<string> split_string(const string &str, const string &delim)
{
    vector<string> strings;
    string tstr;

    string::size_type pos = 0;
    string::size_type prev = 0;
    string::size_type lwhere;
    while ((pos = str.find(delim, prev)) != std::string::npos)
    {
        tstr = str.substr(prev, pos - prev);
        lwhere = 0;
        if (tstr[lwhere] == '\"')
            tstr.erase(lwhere,1);
        lwhere = tstr.length() - 1;
        if (tstr[lwhere] == '\"')
            tstr.erase(lwhere,1);
        strings.push_back(tstr);
        prev = pos + 1;
    }

    // To get the last substring (or only, if delimiter is not found)
    tstr = str.substr(prev);
    lwhere = 0;
    if (tstr[lwhere] == '\"')
        tstr.erase(lwhere,1);
    lwhere = tstr.length() - 1;
    if (tstr[lwhere] == '\"')
        tstr.erase(lwhere,1);
    strings.push_back(tstr);

    return strings;
}

int main(int argc, char ** argv)
{

    if (argc > 1)
    {

        ifstream infile;
        string line_in;
        vector<string> field_data;

        sqlite3 *ppDb;
        sqlite3_stmt *ppStmt;

        try {

            infile.open(argv[1]);

            if (!infile)
                throw "Erro ao abrir arquivo de entrada.";

            string dbname;
            if (argc > 2)
                dbname = argv[2];
            else
                dbname = "work.sqlite";

            if (sqlite3_open(dbname.c_str(), &ppDb) != SQLITE_OK)
                throw "Erro ao abrir/criar base de dados";


           /*
               0 ANO_CMPT,
               1 MES_CMPT,
               2 ESPEC,
               3 N_AIH,
               4 CEP,
               5 IDENT,
               6 MUNIC_RES,
               7 NASC,
               8 PROC_SOLIC,
               9 PROC_REA,
               10 DT_INTER,
               11 DT_SAIDA,
               12 DIAG_PRINC,
               13 DIAG_SECUN,
               14 COBRANCA,
               15 CNES,
               16 FLAG_PROC_REA,
               17 FLAG_DIAGNOSTICO
           */

            const char *init_sql = "drop table if exists aih;"
                             "create table aih(ANO_CMPT, MES_CMPT,ESPEC, N_AIH,"
                             "CEP, IDENT, MUNIC_RES, NASC, PROC_SOLIC,"
                             "PROC_REA, DT_INTER, DT_SAIDA, DIAG_PRINC, DIAG_SECUN,"
                             "COBRANCA, CNES,FLAG_PROC_REA,FLAG_DIAGNOSTICO);";

            if (sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr) != SQLITE_OK)
                throw "Erro ao criar tabela.";

            getline(infile,line_in);

            vector<string> filefldnames = split_string(line_in,";");
            vector<string> fldnames = split_string("\"ANO_CMPT\",\"MES_CMPT\",\"ESPEC\",\"N_AIH\","
                             "\"CEP\",\"IDENT\",\"MUNIC_RES\",\"NASC\",\"PROC_SOLIC\","
                             "\"PROC_REA\",\"DT_INTER\",\"DT_SAIDA\",\"DIAG_PRINC\",\"DIAG_SECUN\","
                             "\"COBRANCA\",\"CNES\",\"FLAG_PROC_REA\",\"FLAG_DIAGNOSTICO\"",",");

            int fldno[18], i, j;

            for (i = 0; i < 18; i++)
                fldno[i] = -1;

            for (i = 0; i < filefldnames.size(); i++)
            {
                for (j = 0; j < 18 ; j++)
                {
                    //cout << filefldnames[i] << " " << fldnames[j] << endl;
                    if (filefldnames[i] == fldnames[j])
                    {
                        fldno[j] = i;
                        break;
                    }
                }
            }

            /*for (i = 0; i < 18; i++)
                if (fldno[i] == -1)
                    throw "Arquivo de entrada nao contem todos os campos necessarios.";*/

            //
            //cout << "Init complete" << endl;

            const char *add_data = "insert into aih"
                            "(ANO_CMPT,"
                            "MES_CMPT,"
                            "ESPEC,"
                            "N_AIH,"
                            "CEP,"
                            "IDENT,"
                            "MUNIC_RES,"
                            "NASC,"
                            "PROC_SOLIC,"
                            "PROC_REA,"
                            "DT_INTER,"
                            "DT_SAIDA,"
                            "DIAG_PRINC,"
                            "DIAG_SECUN,"
                            "COBRANCA,"
                            "CNES,"
                            "FLAG_PROC_REA,"
                            "FLAG_DIAGNOSTICO)"
                            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

            long recs = 0L;

            sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

            while (getline(infile,line_in))
            {
                field_data.clear();
                field_data = split_string(line_in, ";");
                if ((fldno[4] != -1) && (field_data[fldno[4]].length() < 8))
                    field_data[fldno[4]] = string("0") + field_data[fldno[4]];

                //for (int i = 0; i < 17; i++)
                //    cout << i << ":" << fldno[i] <<  ":" << field_data[fldno[i]].c_str() << endl;

                if (sqlite3_prepare_v2(ppDb, add_data, -1, &ppStmt, nullptr) != SQLITE_OK)
                    throw "Erro ao adicionar dados (prepare).";


                for (i = 0; i < 18; i++)
                {
                    if (fldno[i] == -1)
                    {
                        if (sqlite3_bind_text(ppStmt, (i+1), "N/A", 3, SQLITE_STATIC) != SQLITE_OK)
                            throw "Erro ao adicionar dados (bind).";
                    }
                    else
                    {
                        if (sqlite3_bind_text(ppStmt, (i+1), field_data[fldno[i]].c_str(), field_data[fldno[i]].length(), SQLITE_STATIC) != SQLITE_OK)
                            throw "Erro ao adicionar dados (bind).";
                    }

                }
                sqlite3_step(ppStmt);
                sqlite3_finalize(ppStmt);
                cerr << ++recs << '\r';
            };

            sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

            cerr << endl << "Fim." << endl;

            sqlite3_close(ppDb);
            infile.close();

            /*
            sqlite3_prepare_v2(ppDb, "select * from aih;", -1, &ppStmt, nullptr);
            while (sqlite3_step(ppStmt) != SQLITE_DONE)
            {
                for (int col = 0; col < 12; col++)
                    cout << sqlite3_column_text(ppStmt, col) << ';';
                cout << endl;
            }
            sqlite3_finalize(ppStmt);
            */

        } catch (const char *s) {
            cerr << s << endl;
            return -1;
        };
    }
    else
        cerr << "Uso: load_table <arq. entrada> [arq. sqlite]" << endl;

    return 0;
}
