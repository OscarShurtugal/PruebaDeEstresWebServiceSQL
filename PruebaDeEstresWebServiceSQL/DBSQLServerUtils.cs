using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PruebaDeEstresWebServiceSQL
{
    class DBSQLServerUtils
    {
        public static SqlConnection
               GetDBConnection(string datasource, string database, string username, string password)
        {
            //
            // Data Source=TRAN-VMWARE\SQLEXPRESS;Initial Catalog=simplehr;Persist Security Info=True;User ID=sa;Password=12345
            //
            string connString;

            if (username.Length >= 3)
            connString = @"Data Source=" + datasource + ";Initial Catalog="+ database + ";Persist Security Info=True;User ID=" + username + ";Password=" + password;
            else
            connString = @"Data Source=" + datasource + ";Initial Catalog="+ database + ";Integrated Security =True";

            SqlConnection conn = new SqlConnection(connString);

            return conn;
        }

    }
}
