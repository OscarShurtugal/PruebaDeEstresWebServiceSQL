using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PruebaDeEstresWebServiceSQL
{
    class DBUtils
    {
  
            public static SqlConnection GetDBConnection()
            {
                string datasource = @"192.168.205.135\SQLEXPRESS";

                string database = "simplehr";
                string username = "sa";
                string password = "1234";

                return DBSQLServerUtils.GetDBConnection(datasource, database, username, password);
            }
     }

}

