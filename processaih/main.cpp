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

bool loadtable(char * fname, sqlite3 *ppDb)
{
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
        9 FLAG
    */

    bool retval = false;

    cerr << "Criando tabela e indices" << endl;

    const char *init_sql = "drop table if exists aih;"
                     "create table aih(CNES TEXT,CEP TEXT,MUNIC_RES TEXT,NASC TEXT,"
                     "DT_INTER TEXT,DT_SAIDA TEXT,COBRANCA TEXT,N_AIH TEXT,AIHREF TEXT,FLAG TEXT);\n"
                     "drop index if exists orderperm;"
                     "create index orderperm on aih(CNES,MUNIC_RES,NASC,DT_INTER);\n"
                     "drop index if exists ordertransf;"
                     "create index ordertransf on aih(CEP,MUNIC_RES,NASC,DT_INTER);\n;"
                     "drop index if exists orderxport;"
                     "create index orderxport on aih(AIHREF,DT_INTER);"
                     "drop index if exists refaih;"
                     "create index refaih on aih(AIHREF);";

    sqlite3_stmt *ppStmt;

    try {

        ifstream infile;

        infile.open(fname);

        if (!infile)
                throw "Erro ao abrir arquivo de entrada.";

        if (sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr) != SQLITE_OK)
            throw "Erro ao criar tabela.";

        string line_in;
        vector<string> field_data;

        getline(infile,line_in);

        vector<string> filefldnames = split_string(line_in,";");

        vector<string> fldnames = split_string("CNES,CEP,MUNIC_RES,NASC,DT_INTER,DT_SAIDA,"
                                               "COBRANCA,N_AIH,AIHREF,FLAG",",");

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
                        "FLAG"
                        ") "
                        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, \"<init>\");";

        long recs = 0L;

        sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

        cerr << "Iniciando carga dos dados." << endl;

        vector<string> fdata;
        for (int i = 0; i < 9; i++)
                fdata.push_back(string("----"));

        while (getline(infile,line_in))
        {
            //if (++counter > 100)
                //break;
            field_data.clear();

            field_data = split_string(line_in, ";");

            int fldnum = findpos("CEP",filefldnames);

            if ((fldnum != -1) && (field_data[fldnum].length() < 8))
                field_data[fldnum] = string("0") + field_data[fldnum];

            if (sqlite3_prepare_v2(ppDb, add_data, -1, &ppStmt, nullptr) != SQLITE_OK)
                throw "Erro ao adicionar dados (prepare).";

            string fdname;


            for (int i = 0; i < 9; i++)
            {
                fdname = fldnames[i];
                fldnum = findpos(fdname,filefldnames);

                if (fldnum > -1)
                {
                    if (fdname == "COBRANCA")
                        fdata[i] = getcode(field_data[fldnum]);
                    else
                        fdata[i] = field_data[fldnum];
                }
                else if (fdname == "AIHREF")
                {
                    int nfldnum = findpos("N_AIH",filefldnames);
                    fdata[8] = field_data[nfldnum];
                }
                if (sqlite3_bind_text(ppStmt, (i+1), fdata[i].c_str(), fdata[i].length(), SQLITE_STATIC) != SQLITE_OK)
                    throw "Erro ao adicionar dados (bind).";
            }
            sqlite3_step(ppStmt);
            sqlite3_finalize(ppStmt);
            cerr << ++recs << '\r';
        }
        cerr << endl << "Encerrando carga." << endl;
        sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);
        infile.close();
        retval = true;
    } catch(const char *s) {
        cerr << s << endl;
    };
    return retval;
}

bool doperm(sqlite3 *ppDb)
{
    bool retval = false;
    sqlite3_stmt *addstmt, *ppStmt;
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
        9 FLAG
    */

    cerr << "Buscando sequencias." << endl << "Aguarde, ordenando tabela..." << endl;

    long currec;
    long recs = 0L;

    string aih_ref, endcode;

    sqlite3_prepare_v2(ppDb, "select *,rowid from aih order by CNES,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

    try {

        output.open("aihperm.csv");

        output << "CNES,CEP,MUNIC_RES,"
                  "NASC,DT_INTER,DT_SAIDA,"
                  "COBRANCA,N_AIH,AIHREF,"
                  "FLAG";
        output << endl;

        for (int i = 0; i < 10; i++)
        {
            prevvalues.push_back(string(""));
            curvalues.push_back(string(""));
        }

        sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            if (prevvalues[0] == "") // first item
            {
                for (int i = 0; i < 10; i++)
                    prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                for (int i = 0; i < 10; i++)
                {
                    output << prevvalues[i] ;
                    if (i < 9)
                        output << ',';
                    else
                        output << endl;
                }
            }
            else
            {
                for (int i = 0; i < 10; i++)
                    curvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                currec = sqlite3_column_int(ppStmt,10);
                float dt_inter = julian(curvalues[4]);
                float dt_saida = julian(prevvalues[5]);
                float dt_diff = dt_inter - dt_saida;
                if ((prevvalues[0] == curvalues[0]) && //CNES
                    (prevvalues[2] == curvalues[2])  && // MUNIC_RES
                    (prevvalues[3] == curvalues[3]) && // NASC
                    (dt_diff >= 0) && (dt_diff <= 1))
                {
                    curvalues[8] = prevvalues[8];
                    sqlite3_prepare_v2(ppDb, "update aih set AIHREF = ?, FLAG = \"<seq>\" where rowid = ?;", -1, &addstmt, nullptr);
                    sqlite3_bind_text(addstmt, 1, (prevvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // AIHREF
                    sqlite3_bind_int(addstmt, 2, currec);
                    sqlite3_step(addstmt);
                    sqlite3_finalize(addstmt);
                }
                for (int i = 0; i < 10; i++)
                    prevvalues[i] = curvalues[i];

                for (int i = 0; i < 10; i++)
                {
                    output << prevvalues[i];
                    if (i < 9)
                        output << ',';
                    else
                        output << endl;
                }
            }
            cerr << ++recs << '\r';
        }

        sqlite3_finalize(ppStmt);

        sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

        cerr << endl << "Fim da busca." << endl;

        output.close();

        retval = true;

     } catch (const char *s) {
         cerr << "Erro na busca: " << s << endl;
     };
     return retval;
}

int dotransf(sqlite3 *ppDb)
{
    int retval = 0;

    ofstream output;
    output.open("checkxfer.txt", ios::app);
    output << "----- BEGIN -----\n";

    try {
        sqlite3_stmt *addstmt, *ppStmt;

        cerr << "Buscando transferências." << endl;

        cerr << "Aguarde, reordenando..." << endl;

        sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

        sqlite3_prepare_v2(ppDb, "select * from aih order by CEP,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

        if (sqlite3_exec(ppDb, "drop table if exists transf;"
                                "create table transf(ORIGAIHREF TEXT, AIHREF TEXT, DT_INTER);"
                                "drop index if exists txtime;"
                                "create index txtime on transf(DT_INTER);",
                                nullptr, nullptr, nullptr) != SQLITE_OK)
            throw "Erro ao criar tabela";

        long recs = 0L;

        vector<string> prevvalues, curvalues;
        for (int i = 0; i < 10; i++)
        {
            prevvalues.push_back(string("----"));
            curvalues.push_back(string("----"));
        }

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            if (prevvalues[0] == "----")
                for (int i = 0; i < 10; i++)
                    prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
            else
            {
                for (int i = 0; i < 10; i++)
                    curvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
                float dt_inter = julian(curvalues[4]);
                float dt_saida = julian(prevvalues[5]);
                float dt_diff = dt_inter - dt_saida;
                if ((prevvalues[1] == curvalues[1]) && //CEP
                    (prevvalues[2] == curvalues[2])  && // MUNIC_RES
                    (prevvalues[3] == curvalues[3]) && // NASC
                    (dt_diff >= 0) && (dt_diff <= 1) &&
                    (prevvalues[6] == "3") && // COBRANCA -> transf
                    (curvalues[8] != prevvalues[8])) // se AIHREF for igual, não precisa mudar
                {
                    sqlite3_prepare_v2(ppDb, "insert into transf(ORIGAIHREF,AIHREF,DT_INTER) values(?, ?, ?);", -1, &addstmt, nullptr);
                    if (curvalues[9] != "<xfer>") // Nao e' parte de um cluster de transferencia
                    {
                        sqlite3_bind_text(addstmt, 1, (curvalues[8].c_str()), curvalues[8].length(), SQLITE_STATIC); // ORIGAIHREF
                        sqlite3_bind_text(addstmt, 2, (prevvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // AIHREF
                        sqlite3_bind_text(addstmt, 3, (prevvalues[4].c_str()), prevvalues[4].length(), SQLITE_STATIC); // DT_INTER
                        output << curvalues[8] << "," << prevvalues[8] << "," << prevvalues[4] << "down" << endl;
                    }
                    else // Se for, e' o cluster anterior que vai ser adicionado
                    {
                        sqlite3_bind_text(addstmt, 1, (prevvalues[8].c_str()), prevvalues[8].length(), SQLITE_STATIC); // ORIGAIHREF
                        sqlite3_bind_text(addstmt, 2, (curvalues[8].c_str()), curvalues[8].length(), SQLITE_STATIC); // AIHREF
                        sqlite3_bind_text(addstmt, 3, (curvalues[4].c_str()), curvalues[4].length(), SQLITE_STATIC); // DT_INTER
                        output << prevvalues[8] << "," << curvalues[8] << "," << curvalues[4] << "up" << endl;
                    }


                    sqlite3_step(addstmt);
                    sqlite3_finalize(addstmt);
                    retval++;
                }
                for (int i = 0; i < 10; i++)
                    prevvalues[i] = curvalues[i];
            }
            cerr << ++recs << '\r';
        }

        sqlite3_finalize(ppStmt);
        sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

        output << "----- END -----\n";

        output.close();

        cerr << endl << "Fim da busca." << endl;
    } catch (const char *s) {
        retval = -1;
        cerr << "Erro na busca: " << s << endl;
    };
    return retval;
}

bool reconcile(sqlite3 *ppDb)
{
    bool retval = false;

    ofstream output;
    output.open("recx.txt", ios::app);

    try {
        sqlite3_stmt *updstmt, *addstmt, *ppStmt;
        sqlite3_prepare_v2(ppDb, "select * from transf order by DT_INTER;", -1, &ppStmt, nullptr);
        sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

        long currec, recs = 0L;

        vector<string> prevvalues, curvalues;
        for (int i = 0; i < 3; i++)
            prevvalues.push_back(string("----"));

        cerr << "Recompondo sequencias." << endl;

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            for (int i = 0; i < 3; i++)
                prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));

            sqlite3_prepare_v2(ppDb, "select rowid from aih where AIHREF = ? order by AIHREF,DT_INTER;", -1, &addstmt, nullptr);
            sqlite3_bind_text(addstmt, 1, (prevvalues[0].c_str()), prevvalues[0].length(), SQLITE_STATIC); // ORIGAIHREF

            while (sqlite3_step(addstmt) != SQLITE_DONE)
            {
                currec = sqlite3_column_int(addstmt,0);

                sqlite3_prepare_v2(ppDb, "update aih set AIHREF = ?, FLAG = \"<xfer>\" where rowid = ?;",
                                    -1, &updstmt, nullptr);
                sqlite3_bind_text(updstmt, 1, (prevvalues[1].c_str()), prevvalues[1].length(), SQLITE_STATIC); // AIHREF
                sqlite3_bind_int(updstmt, 2, currec);
                sqlite3_step(updstmt);
                sqlite3_finalize(updstmt);
                cerr << ".";
                output << currec << endl;
            }
            cerr << '\r' << ++recs << '\r';
            sqlite3_finalize(addstmt);
        }

        sqlite3_finalize(ppStmt);

        sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

        output.close();

        cerr << endl << "Fim da recomposicao." << endl;
        retval = true;
    } catch (const char *s) {
        cerr << "Erro na recomposicao: " << s << endl;
    }
    return retval;
}

void exportdata(sqlite3 *ppDb)
{
    cerr << "Exportando dados..." << endl;

    ofstream output;
    sqlite3_stmt *ppStmt;

    output.open("aihpermtransf.csv");
    output << "CNES,CEP,MUNIC_RES,"
              "NASC,DT_INTER,DT_SAIDA,"
              "COBRANCA,N_AIH,AIHREF,"
              "FLAG,NORDEM";
    output << endl;

    cerr << "Aguarde, reordenando..." << endl;

    sqlite3_prepare_v2(ppDb, "select * from aih order by AIHREF,DT_INTER;", -1, &ppStmt, nullptr);

    long recs = 0L;

    vector<string> prevvalues;
    for (int i = 0; i < 10; i++)
        prevvalues.push_back(string("----"));

    string aihref = prevvalues[8];
    long nordem = 0;

    while (sqlite3_step(ppStmt) != SQLITE_DONE)
    {
            for (int i = 0; i < 10; i++)
                prevvalues[i] = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));
            if (aihref == "----")
                aihref = prevvalues[8];
            if (aihref == prevvalues[8])
                nordem++;
            else
            {
                aihref = prevvalues[8];
                nordem = 1;
            }
            for (int i = 0; i < 10; i++)
                output << prevvalues[i] << ',';
            output << nordem << endl;
            cerr << ++recs << '\r';
     }

     cerr << ++recs << endl << "Fim." << endl;
}

int main(int argc, char ** argv)
{
    if (argc > 1)
    {
        sqlite3 *ppDb;

        try {

            string dbname;
            if (argc > 2)
                dbname = argv[2];
            else
                dbname = "work.sqlite";

            if (sqlite3_open(dbname.c_str(), &ppDb) != SQLITE_OK)
                throw "Erro ao abrir/criar base de dados";

            if (! loadtable(argv[1], ppDb)) throw "Encerrando.";

            if (! doperm(ppDb)) throw "Encerrando.";

            int res;
            while ((res = dotransf(ppDb)))
            {
                if (res == -1) throw "Encerrando.";
                if (res)
                {
                    cerr << "Encontradas " << res << " transferencias." << endl;
                    if (! reconcile(ppDb)) throw "Encerrando.";
                }
                else
                    cerr << "Nenhuma transferencia encontrada" << endl;
            }

            exportdata(ppDb);

        } catch (const char *s) {
            cerr << s << endl;
            return -1;
        };
    }
    else
        cerr << "Uso: processaih <arq. entrada> [arq. sqlite]" << endl;
    return 0;
}
