#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <fstream>
#include <cstring>
#include <math.h>
#include <stdlib.h>
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

string getcode(string cobranca)
{
    string codigo;
    if ((cobranca >= "11" && cobranca <= "19") || (cobranca >= "61" && cobranca <= "64"))
        codigo = "1"; //alta
    else if (cobranca >= "21" && cobranca <= "29")
        codigo = "2"; //perm
    else if (cobranca >= "31" && cobranca <= "32")
        codigo = "3"; //transf
    else if ((cobranca >= "41" && cobranca <= "43") || (cobranca >= "65" && cobranca <= "67"))
        codigo = "4"; //obito
    else if (cobranca >= "51" && cobranca <= "59")
        codigo = "5"; //admin
    else
        codigo = "0";
    return codigo;
}

int findpos(string what, vector<string> where)
{
    int i;
    for (i = 0; i < where.size(); i++)
        if (where[i] == what)
            break;
    if (i >= where.size())
        i = -1;
    return i;
}

float julian(string datestr)
{
   float monthv, yearv, dayv, y1;

   yearv = atof(datestr.substr(0,4).c_str());
   monthv = atof(datestr.substr(4,2).c_str());
   dayv = atof(datestr.substr(6,2).c_str());

   y1 = yearv + ( monthv - 2.85 ) / 12;
   return( floor( floor( floor( 367.0 * y1 ) - floor( y1 ) - 0.75 * floor( y1 )
      + dayv ) - 0.75 * 2.0 ) + 1721115.0 );
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
                0 CNES
                1 CEP
                2 MUNIC_RES
                3 NASC
                4 DT_INTER
                5 DT_SAIDA
                6 COBRANCA
                7 N_AIH
                8 AIHREF
                9 AIHREFANO
                10 FLAG
                11 NORDEM
           */

            const char *init_sql = "drop table if exists aih;"
                             "create table aih(CNES TEXT,CEP TEXT,MUNIC_RES TEXT,NASC TEXT,DT_INTER TEXT,DT_SAIDA TEXT,COBRANCA TEXT,N_AIH TEXT,AIHREF TEXT,AIHREFANO TEXT,FLAG TEXT,NORDEM TEXT);\n";
                             //"create index orderperm on aih(CNES,MUNIC_RES,NASC,DT_INTER);\n"
                             //"create index ordertransf on aih(CEP,MUNIC_RES,NASC,DT_INTER);\n;"
                             //"create index orderseqtransf on aih(AIHREF,DT_INTER)";

            if (sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr) != SQLITE_OK)
                throw "Erro ao criar tabela.";

            getline(infile,line_in);

            vector<string> filefldnames = split_string(line_in,";");

            vector<string> fldnames = split_string("\"CNES\",\"CEP\",\"MUNIC_RES\",\"NASC\",\"DT_INTER\",\"DT_SAIDA\",\"COBRANCA\",\"N_AIH\",\"AIHREF\",\"AIHREFANO\",\"FLAG\",\"NORDEM\"",",");

            int fldno[12];

            for (int i = 0; i < 12; i++)
                fldno[i] = findpos(fldnames[i], filefldnames);

            const char *add_data = "insert into aih"
                            "("
                            "CNES,"
                            "CEP,"
                            "MUNIC_RES,"
                            "NASC,"
                            "DT_INTER,"
                            "DT_SAIDA,"
                            "COBRANCA,"
                            "N_AIH,"
                            "AIHREF,"
                            "AIHREFANO,"
                            "FLAG,"
                            "NORDEM"
                            ") "
                            "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \"<init>\", \"1\");";

            long recs = 0L;

            sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

            cerr << "Iniciando carga dos dados." << endl;

            while (getline(infile,line_in))
            {
                //if (++counter > 100)
                    //break;
                field_data.clear();

                field_data = split_string(line_in, ";");

                int fldno = findpos("CEP",filefldnames);

                if ((fldno != -1) && (field_data[fldno].length() < 8))
                    field_data[fldno] = string("0") + field_data[fldno];

                if (sqlite3_prepare_v2(ppDb, add_data, -1, &ppStmt, nullptr) != SQLITE_OK)
                    throw "Erro ao adicionar dados (prepare).";

                string fname;

                string nada = string("nada");

                vector<string> fdata;

                for (int i = 0; i < 12; i++)
                    fdata.push_back(nada);

                for (int i = 0; i < 10; i++)
                {
                    fname = fldnames[i];
                    fldno = findpos(fname,filefldnames);

                    if (fldno > -1)
                    {
                        if (fname == "COBRANCA")
                            fdata[i] = getcode(field_data[fldno]);
                        else
                            fdata[i] = field_data[fldno];
                    }
                    else if (fname == "AIHREF")
                    {
                        int nfldno = findpos("N_AIH",filefldnames);
                        fdata[8] = field_data[nfldno];
                    }
                    else if (fname == "AIHREFANO")
                    {
                        int nfldno = findpos("DT_INTER",filefldnames);
                        fdata[9] = field_data[nfldno].substr(0,4);
                    }
                    if (sqlite3_bind_text(ppStmt, (i+1), fdata[i].c_str(), fdata[i].length(), SQLITE_STATIC) != SQLITE_OK)
                        throw "Erro ao adicionar dados (bind).";
                    //cerr << fdata[i].c_str() << " ";
                }
                //cerr << endl;
                try {
                    sqlite3_step(ppStmt);
                    sqlite3_finalize(ppStmt);
                } catch(...) {
                    cerr << "oops..." << endl;
                }
                cerr << ++recs << '\r';
            };

            cerr << endl << "Encerrando carga." << endl;

            sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

            infile.close();

            sqlite3_stmt *addstmt;
            ofstream output;
            vector<string> prevvalues, curvalues;

            /*
                0 CNES
                1 CEP
                2 MUNIC_RES
                3 NASC
                4 DT_INTER
                5 DT_SAIDA
                6 COBRANCA
                7 N_AIH
                8 AIHREF
                9 AIHREFANO
                10 FLAG
                11 NORDEM
            */

            cerr << "Aguarde, ordenando tabela..." << endl;

            long currec;
            recs = 0L;

            string aih_ref, endcode;

            sqlite3_prepare_v2(ppDb, "select *,rowid from aih order by CNES,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

            output.open("aihperm.csv");

            output << "CNES,CEP,MUNIC_RES,"
                      "NASC,DT_INTER,DT_SAIDA,"
                      "COBRANCA,N_AIH,AIHREF,"
                      "AIHREFANO,FLAG,NORDEM";
            output << endl;

            string nada = string("nada");
            for (int i = 0; i < 12; i++)
            {
                prevvalues.push_back(nada);
                curvalues.push_back(nada);
            }

            sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

            while (sqlite3_step(ppStmt) != SQLITE_DONE)
            {
                if (prevvalues[0] == "nada") // first item
                {
                    for (int i = 0; i < 12; i++)
                        prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                    for (int i = 0; i < 12; i++)
                    {
                        output << "\"" << prevvalues[i] << "\"";
                        if (i < 11)
                            output << ",";
                        else
                            output << endl;
                    }
                }
                else
                {
                    for (int i = 0; i < 12; i++)
                        curvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                    currec = sqlite3_column_int(ppStmt,12);
                    float dt_inter = julian(curvalues[4]);
                    float dt_saida = julian(prevvalues[5]);
                    float dt_diff = dt_inter - dt_saida;
                    if ((prevvalues[0] == curvalues[0]) && //CNES
                        (prevvalues[2] == curvalues[2])  && // MUNIC_RES
                        (prevvalues[3] == curvalues[3]) && // NASC
                        (dt_diff >= 0) && (dt_diff <= 1))
                    {
                        curvalues[8] = prevvalues[8];
                        curvalues[9] = prevvalues[9];
                        curvalues[10] = "<seq>";
                        sqlite3_prepare_v2(ppDb, "update aih set AIHREF = ?, AIHREFANO = ?, FLAG = \"<seq>\", NORDEM = ? where rowid = ?;", -1, &addstmt, nullptr);
                        sqlite3_bind_text(addstmt, 1, (prevvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // AIHREF
                        sqlite3_bind_text(addstmt, 2, (prevvalues[9].c_str()), prevvalues[9].length(), SQLITE_STATIC); // AIHREFANO
                        long nordem = stoi(prevvalues[11]) + 1;
                        curvalues[11] = to_string(nordem);
                        sqlite3_bind_text(addstmt, 3, (curvalues[11].c_str()), curvalues[11].length(), SQLITE_STATIC); // NORDEM
                        sqlite3_bind_int(addstmt, 4, currec);
                        sqlite3_step(addstmt);
                        sqlite3_finalize(addstmt);
                    }
                    for (int i = 0; i < 12; i++)
                        prevvalues[i] = curvalues[i];

                    for (int i = 0; i < 12; i++)
                    {
                        output << "\"" << prevvalues[i] << "\"";
                        if (i < 11)
                            output << ",";
                        else
                            output << endl;
                    }
                }
                cerr << ++recs << '\r';

            }

            sqlite3_finalize(ppStmt);

            sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

            cerr << endl << "Fim da fase 1." << endl;

            output.close();

            output.open("aihtransf.csv");

            output << "ORIGAIHREF,AIHREF,"
                      "AIHREFANO,NORDEM";
            output << endl;

            cerr << "Aguarde, reordenando..." << endl;

            sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

            sqlite3_prepare_v2(ppDb, "select * from aih order by CEP,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

            if (sqlite3_exec(ppDb, "drop table if exists transf;"
                                    "create table transf(ORIGAIHREF TEXT, AIHREF TEXT, AIHREFANO TEXT, NORDEM TEXT);",
                                    nullptr, nullptr, nullptr) != SQLITE_OK)
                cerr << "Erro ao criar tabela" << endl;

            recs = 0L;

            for (int i = 0; i < 12; i++)
                prevvalues.push_back(nada);

            while (sqlite3_step(ppStmt) != SQLITE_DONE)
            {
                if (prevvalues[0] == "nada")
                    for (int i = 0; i < 12; i++)
                        prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                else
                {
                    for (int i = 0; i < 12; i++)
                        curvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                    float dt_inter = julian(curvalues[4]);
                    float dt_saida = julian(prevvalues[5]);
                    float dt_diff = dt_inter - dt_saida;
                    if ((prevvalues[1] == curvalues[1]) && //CEP
                        (prevvalues[2] == curvalues[2])  && // MUNIC_RES
                        (prevvalues[3] == curvalues[3]) && // NASC
                        (dt_diff >= 0) && (dt_diff <= 1) &&
                        (prevvalues[6] == "3"))  // COBRANCA -> transf
                    {
                        sqlite3_prepare_v2(ppDb, "insert into transf(ORIGAIHREF,AIHREF,AIHREFANO,NORDEM) values(?, ?, ?, ?);", -1, &addstmt, nullptr);
                        sqlite3_bind_text(addstmt, 1, (curvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // ORIGAIHREF
                        sqlite3_bind_text(addstmt, 2, (prevvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // AIHREF
                        sqlite3_bind_text(addstmt, 3, (prevvalues[9].c_str()), prevvalues[9].length(), SQLITE_STATIC); // AIHREFANO
                        sqlite3_bind_text(addstmt, 4, (prevvalues[11].c_str()), prevvalues[11].length(), SQLITE_STATIC); // NORDEM
                        sqlite3_step(addstmt);
                        sqlite3_finalize(addstmt);
                        output << currec << "," << curvalues[8] << "," << prevvalues[8] << "," << prevvalues[9] << "," << prevvalues[11] << endl;
                    }
                    for (int i = 0; i < 12; i++)
                        prevvalues[i] = curvalues[i];

                    cerr << ++recs << '\r';
                }
            }

            sqlite3_finalize(ppStmt);

            sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

            output.close();
            cerr << endl << "Fim da fase 2." << endl;

            sqlite3_prepare_v2(ppDb, "select * from transf;", -1, &ppStmt, nullptr);

            sqlite3_stmt * updstmt;

            sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);
            cerr << "Aguarde, criando indice..." << endl;
            sqlite3_exec(ppDb, "drop index if exists refaih;"
                         "create index refaih on aih(AIHREF);",
                         nullptr, nullptr, nullptr);

            recs = 0;

            while (sqlite3_step(ppStmt) != SQLITE_DONE)
            {
                for (int i = 0; i < 4; i++)
                    prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));

                long nordem = stoi(prevvalues[3]);

                sqlite3_prepare_v2(ppDb, "select rowid, DT_INTER from aih where AIHREF = ? order by DT_INTER;", -1, &addstmt, nullptr);
                sqlite3_bind_text(addstmt, 1, (prevvalues[0].c_str()), prevvalues[0].length(), SQLITE_STATIC); // ORIGAIHREF

                while (sqlite3_step(addstmt) != SQLITE_DONE)
                {
                    //for (int i = 0; i < 12; i++)
                        //curvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(addstmt, i)));
                    currec = sqlite3_column_int(addstmt,0);

                    sqlite3_prepare_v2(ppDb, "update aih set AIHREF = ?, AIHREFANO = ?, FLAG = \"<xfer>\", NORDEM = ? where rowid = ?;",
                                        -1, &updstmt, nullptr);
                    sqlite3_bind_text(updstmt, 1, (prevvalues[1].c_str()), prevvalues[1].length(), SQLITE_STATIC); // AIHREF
                    sqlite3_bind_text(updstmt, 2, (prevvalues[2].c_str()), prevvalues[2].length(), SQLITE_STATIC); // AIHREFANO
                    prevvalues[3] = to_string(++nordem);
                    sqlite3_bind_text(updstmt, 3, (prevvalues[3].c_str()), prevvalues[3].length(), SQLITE_STATIC); // NORDEM
                    sqlite3_bind_int(updstmt, 4, currec);
                    sqlite3_step(updstmt);
                    sqlite3_finalize(updstmt);
                    cerr << ".";
                }
                cerr << ++recs << '\r';
                sqlite3_finalize(addstmt);
            }

            sqlite3_finalize(ppStmt);

            sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

            cerr << endl << "Fim da fase 3." << endl;

            cerr << "Exportando dados..." << endl;

            output.open("aihpermtransf.csv");
            output << "CNES,CEP,MUNIC_RES,"
                      "NASC,DT_INTER,DT_SAIDA,"
                      "COBRANCA,N_AIH,AIHREF,"
                      "AIHREFANO,FLAG,NORDEM";
            output << endl;

            cerr << "Aguarde, reordenando..." << endl;

            sqlite3_prepare_v2(ppDb, "select * from aih order by AIHREF,DT_INTER;", -1, &ppStmt, nullptr);

            recs = 0;
            while (sqlite3_step(ppStmt) != SQLITE_DONE)
            {
                    for (int i = 0; i < 12; i++)
                        prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                    for (int i = 0; i < 12; i++)
                    {
                        output << "\"" << prevvalues[i] << "\"";
                        if (i < 11)
                            output << ",";
                        else
                            output << endl;
                    }
                    cerr << ++recs << '\r';
             }

             cerr << endl << "Fim." << endl;


        } catch (const char *s) {
            cerr << s << endl;
            return -1;
        };

    }
    else
        cerr << "Uso: processaih <arq. entrada> [arq. sqlite]" << endl;

    return 0;
}
