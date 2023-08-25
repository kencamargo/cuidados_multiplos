#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <vector>
#include <math.h>
#include <stdlib.h>
#include "sqlite3.h"

using namespace std;

string cvt_code(string incode)
{
    string outcode;

    if ((incode >= "11" && incode <= "19") || (incode >= "61" && incode <= "64"))
        outcode = "3";
    else if (incode >= "21" && incode <= "29")
        outcode = "1";
    else if (incode >= "31" && incode <= "32")
        outcode = "2";
    else if ((incode >= "41" && incode <= "43") || (incode >= "65" && incode <= "67"))
        outcode = "4";
    else if (incode >= "51" && incode <= "59")
        outcode = "5";
    return outcode;
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
        sqlite3 *ppDb;
        ofstream output;

        try {
        //cout << argv[1] << endl;

        ifstream testing;
        testing.open(argv[1]);
        if (!testing)
        {
            cerr << "Arquivo nao existe, encerrando." << endl;
            throw -1;
        }
        testing.close();

        sqlite3_open(argv[1], &ppDb);

        sqlite3_exec(ppDb, "begin transaction;", nullptr, nullptr, nullptr);

        sqlite3_stmt *ppStmt, *addstmt;
        vector<string> prevvalues;

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

        sqlite3_prepare_v2(ppDb, "select * from aih where IDENT != \"5\" order by CNES,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

        cerr << "Aguarde, ordenando..." << endl;

        /*if (argc > 2)
            output.open(argv[2]);
        else */
            output.open("perm.csv");

        const char *init_sql = "drop table if exists perm;"
                             "create table perm(AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG);";
        sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);

        const char *add_data = "insert into perm(AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG) values(?, ?, ?, ?, ?, ?, ?);";

        string aih_ref, endcode;
        long recs = 0L;

        prevvalues.clear();

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            if (prevvalues.size())
            {
                if ((prevvalues[15] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 15)))) && //CNES
                    (prevvalues[6] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 6)))) && // MUNIC_RES
                    (prevvalues[7] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 7)))))// NASC
                {
                    float dt_inter = julian(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10)));
                    float dt_saida = julian(prevvalues[11]);
                    // 10 DT_INTER
                    // 11 DT_SAIDA
                    float dt_diff = dt_inter - dt_saida;

                    endcode = cvt_code(prevvalues[14]);
                    if ((endcode == "1") && (dt_diff >= 0) && (dt_diff <= 1))
                    {
                        //output << prevvalues[0] << " faz par com " << sqlite3_column_text(ppStmt, 0) << " Cod. " << ncode << endl;
                        output << aih_ref << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                             << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<seq>"
                             << endl;

                         //cerr << "*" << endl;
                         sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                         sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                         const char *datastr;
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                         sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                         sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                         sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                         sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                         string datacode = cvt_code(string(datastr));
                         sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                         const char *flag = "<seq>";
                         sqlite3_bind_text(addstmt, 7, flag, 5, SQLITE_STATIC);
                         sqlite3_step(addstmt);
                         sqlite3_finalize(addstmt);
                    }
                    else
                    {
                        endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                        if (endcode == "1")
                        {
                            aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                            output << aih_ref << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                                 << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<init>"
                                 << endl;

                            sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                            sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                            const char *datastr;
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                            sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                            sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                            sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                            sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                            string datacode = cvt_code(string(datastr));
                            sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                            const char *flag = "<init>";
                            sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                            sqlite3_step(addstmt);
                            sqlite3_finalize(addstmt);
                        }
                    }
                }
                else
                {
                    endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                    if (endcode == "1")
                    {
                        aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                        output << aih_ref << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                             << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<init>"
                             << endl;

                        sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                        sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                        const char *datastr;
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                        sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                        sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                        sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                        sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                        string datacode = cvt_code(string(datastr));
                        sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                        const char *flag = "<init>";
                        sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                        sqlite3_step(addstmt);
                        sqlite3_finalize(addstmt);
                    }
                }
            }
            else
            {
                // init
                cerr << "Processando..." << endl;
                output << "AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG" << endl;

                endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                if (endcode == "1")
                {
                    aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                    output << aih_ref << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                         << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<init>"
                         << endl;

                    sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                    sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                    const char *datastr;
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                    sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                    sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                    sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                    sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                    string datacode = cvt_code(string(datastr));
                    sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                    const char *flag = "<init>";
                    sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                    sqlite3_step(addstmt);
                    sqlite3_finalize(addstmt);
                }
            }
            prevvalues.clear();
            for (int i = 0; i < 18; i++)
                prevvalues.push_back(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));

            cerr << ++recs << '\r';

        }

        sqlite3_finalize(ppStmt);
        //sqlite3_close(ppDb);
        //output.close();
        cerr << endl << "Fim da fase 1." << endl;

        output.close();

        /* if (argc > 3)
            output.open(argv[3]);
        else */
            output.open("transf.csv");

        sqlite3_prepare_v2(ppDb, "select * from aih where IDENT != \"5\" order by CEP,MUNIC_RES,NASC,DT_INTER;", -1, &ppStmt, nullptr);

        cerr << "Aguarde, ordenando..." << endl;



        init_sql = "drop table if exists transf;"
                   "create table transf(AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG);";
        sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);

        add_data = "insert into transf(AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG) values(?, ?, ?, ?, ?, ?, ?);";

        recs = 0L;

        prevvalues.clear();

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            if (prevvalues.size())
            {
                if ((prevvalues[4] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 4)))) && // CEP
                    (prevvalues[6] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 6)))) && // MUNIC_RES
                    (prevvalues[7] == string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 7)))))// NASC
                {
                    float dt_inter = julian(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10)));
                    float dt_saida = julian(prevvalues[11]);
                    // 10 DT_INTER
                    // 11 DT_SAIDA
                    float dt_diff = dt_inter - dt_saida;

                    endcode = cvt_code(prevvalues[14]);
                    if ((endcode == "2") && (dt_diff >= 0) && (dt_diff <= 1))
                    {
                        //output << prevvalues[0] << " faz par com " << sqlite3_column_text(ppStmt, 0) << " Cod. " << ncode << endl;
                        output << aih_ref << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                             << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<trf>"
                             << endl;

                         //cerr << "*" << endl;
                         sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                         sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                         const char *datastr;
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                         sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                         sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                         sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                         datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                         sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                         string datacode = cvt_code(string(datastr));
                         sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                         const char *flag = "<seq>";
                         sqlite3_bind_text(addstmt, 7, flag, 5, SQLITE_STATIC);
                         sqlite3_step(addstmt);
                         sqlite3_finalize(addstmt);
                    }
                    else
                    {
                        endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                        if (endcode == "2")
                        {
                            aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                            output << aih_ref << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                                 << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                                 << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<orig>"
                                 << endl;

                            sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                            sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                            const char *datastr;
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                            sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                            sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                            sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                            datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                            sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                            string datacode = cvt_code(string(datastr));
                            sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                            const char *flag = "<orig>";
                            sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                            sqlite3_step(addstmt);
                            sqlite3_finalize(addstmt);
                        }
                    }
                }
                else
                {
                    endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                    if (endcode == "2")
                    {
                        aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                        output << aih_ref << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                             << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                             << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<orig>"
                             << endl;

                        sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                        sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                        const char *datastr;
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                        sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                        sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                        sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                        datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                        sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                        string datacode = cvt_code(string(datastr));
                        sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                        const char *flag = "<orig>";
                        sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                        sqlite3_step(addstmt);
                        sqlite3_finalize(addstmt);
                    }
                }
            }
            else
            {
                // init
                cerr << "Processando..." << endl;
                output << "AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG" << endl;

                endcode = cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))));
                if (endcode == "2")
                {
                    aih_ref = string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3)));
                    output << aih_ref << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11))) << ","
                         << string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14))) << ","
                         << cvt_code(string(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14)))) << ",<orig>"
                         << endl;

                    sqlite3_prepare_v2(ppDb, add_data, -1, &addstmt, nullptr);
                    sqlite3_bind_text(addstmt, 1, aih_ref.c_str(), aih_ref.length(), SQLITE_STATIC);
                    const char *datastr;
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 3));
                    sqlite3_bind_text(addstmt, 2, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 10));
                    sqlite3_bind_text(addstmt, 3, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 11));
                    sqlite3_bind_text(addstmt, 4, datastr, strlen(datastr), SQLITE_STATIC);
                    datastr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 14));
                    sqlite3_bind_text(addstmt, 5, datastr, strlen(datastr), SQLITE_STATIC);
                    string datacode = cvt_code(string(datastr));
                    sqlite3_bind_text(addstmt, 6, datacode.c_str(), 1, SQLITE_STATIC);
                    const char *flag = "<orig>";
                    sqlite3_bind_text(addstmt, 7, flag, 6, SQLITE_STATIC);
                    sqlite3_step(addstmt);
                    sqlite3_finalize(addstmt);
                }
            }
            prevvalues.clear();
            for (int i = 0; i < 18; i++)
                prevvalues.push_back(reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, i)));

            cerr << ++recs << '\r';

        }

        sqlite3_finalize(ppStmt);
        output.close();
        cerr << endl << "Fim da fase 2." << endl;

        sqlite3_prepare_v2(ppDb, "select * from perm where CODIGO = \"2\";", -1, &ppStmt, nullptr);
        const char *aihstr, *refaihstr, *newref;

        //sqlite3_stmt *tmp, *group;

        //

        recs = 0L;

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
             cerr << ++recs << '\r';

             refaihstr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 0));
             aihstr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 1));

             sqlite3_prepare_v2(ppDb, "select * from transf where AIH = \"?\";", -1, &addstmt, nullptr);
             sqlite3_bind_text(addstmt, 1, aihstr, strlen(aihstr), SQLITE_STATIC);
             if (sqlite3_step(addstmt) != SQLITE_DONE)
                newref = reinterpret_cast<const char *>(sqlite3_column_text(addstmt, 0));
             else
                newref = nullptr;
             sqlite3_finalize(addstmt);
             if (newref != nullptr)
             {
                // string command = "delete from transf where AIH = " + string(aihstr) + ";";
                // init_sql = reinterpret_cast<const char *>(command.c_str());
                // sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);
                string command = "update transf set AIH_REF = \"" + string(refaihstr) + "\", FLAG = \"<update>\" where AIH_REF = \"" + string(newref) + "\";";
                init_sql = reinterpret_cast<const char *>(command.c_str());
                sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);
             }
        }

        sqlite3_finalize(ppStmt);

        cerr << endl << "Fim da fase 3." << endl ;

        //
        /*
        output.open("transfmod.csv");

        cerr << "Exportando parcial ..." << endl;

        output << "AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG" << endl;

        sqlite3_prepare_v2(ppDb, "select * from transf order by AIH_REF,DT_INTER;", -1, &ppStmt, nullptr);
        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            cerr << ++recs << '\r';

            for (int i = 0; i < 7; i++)
            {
                output << sqlite3_column_text(ppStmt, i);
                if (i < 6)
                    output << ",";
                else
                    output << endl;
            }
            cerr << ++recs << '\r';
        }
        sqlite3_finalize(ppStmt);
        output.close();
        */
        //


        sqlite3_prepare_v2(ppDb, "select * from transf where CODIGO != \"2\" and FLAG != \"<update>\" ;", -1, &ppStmt, nullptr);
        // procura possiveis sequencias que não haviam sido localizadas antes (começaram com uma transferência que não foi encontrada no passo anterior)


        recs = 0L;

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
             cerr << ++recs << '\r';

             refaihstr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 0));
             aihstr = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 1));

             sqlite3_prepare_v2(ppDb, "select * from perm where AIH = \"?\";", -1, &addstmt, nullptr);
             sqlite3_bind_text(addstmt, 1, aihstr, strlen(aihstr), SQLITE_STATIC);
             if (sqlite3_step(addstmt) != SQLITE_DONE)
                newref = reinterpret_cast<const char *>(sqlite3_column_text(addstmt, 0));
             else
                newref = nullptr;
             sqlite3_finalize(addstmt);
             if (newref != nullptr)
             {
                if (strcmp(newref,refaihstr))
                {
                    string command = "update perm set AIH_REF = \"" + string(refaihstr) + "\" where AIH_REF = \"" + string(newref) + "\";";
                    init_sql = reinterpret_cast<const char *>(command.c_str());
                    sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);
                }
             }
        }

        sqlite3_finalize(ppStmt);

        cerr << endl << "Fim da fase 4." << endl ;

        cerr << "Eliminando duplicidades." << endl;

        sqlite3_exec(ppDb, "delete from transf where AIH in (select AIH from perm);", nullptr, nullptr, nullptr);

        init_sql = "drop table if exists permtransf;"
                   "create table permtransf(AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG);"
                   "insert into permtransf select * from perm;"
                   "insert into permtransf select * from transf;"
                   ;

        cerr << "Agregando tabelas." << endl;

        sqlite3_exec(ppDb, init_sql, nullptr, nullptr, nullptr);

        //



        recs = 0L;

        /*if (argc > 4)
            output.open(argv[4]); */
        if (argc > 2)
            output.open(argv[2]);
        else
            output.open("permtransf.csv");

        cerr << "Exportando..." << endl;

        output << "AIH_REF,AIH,DT_INTER,DT_SAIDA,COBRANCA,CODIGO,FLAG,NORDEM" << endl;

        sqlite3_prepare_v2(ppDb, "select * from permtransf order by AIH_REF,DT_INTER;", -1, &ppStmt, nullptr);

        string refaih = "", compaih;
        int numrecscluster;

        while (sqlite3_step(ppStmt) != SQLITE_DONE)
        {
            cerr << ++recs << '\r';

            if (refaih == "")
            {
                refaih = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 0));
                numrecscluster = 1;
            }
            /*
            for (int i = 0; i < 7; i++)
            {
                output << sqlite3_column_text(ppStmt, i);
                if (i < 6)
                    output << ",";
                else
                    output << endl;
            }
            */
            for (int i = 0; i < 7; i++)
            {
                output << sqlite3_column_text(ppStmt, i);
                output << ",";
            }

            compaih = reinterpret_cast<const char *>(sqlite3_column_text(ppStmt, 0));

            if (compaih != refaih)
            {
                refaih = compaih;
                numrecscluster = 1;
            }

            output << numrecscluster++ << endl;

            cerr << ++recs << '\r';
        }
        sqlite3_finalize(ppStmt);
        output.close();
        //

        sqlite3_exec(ppDb, "end transaction;", nullptr, nullptr, nullptr);

        sqlite3_close(ppDb);
        output.close();
        cerr << endl << "Fim." << endl;

        } catch (...) {
            sqlite3_close(ppDb);
            output.close();
            cerr << endl << "Erro na execucao" << endl;
            return -1;
        };
    }
    else
        cerr << "Uso: sqlite_script <arq. sqlite> [arq.combinado]" << endl;

    return 0;
}
