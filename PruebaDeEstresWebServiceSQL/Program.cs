using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PruebaDeEstresWebServiceSQL
{
    class Program
    {
        static void Main(string[] args)
        {

            Console.WriteLine("Getting db connection...");
            SqlConnection con = DBSQLServerUtils.GetDBConnection("MX17-MEX018012", "Prueba", "", "");
            //Data Source = MX17 - MEX018012; Initial Catalog = Prueba; Integrated Security = True
            try
            {
                Console.WriteLine("Opening connection...");
                con.Open();
                Console.WriteLine("Connection successful!");

                try
                {
                    //QueryDNs(con);
                    //CreateTempTable(con);
                    usingTempVariableSQLTest(con);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e.Message);
                    Console.WriteLine(e.StackTrace);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: " + e.Message);
            }

            Console.ReadLine();
        }

        /// <summary>
        /// Este metodo es una prueba de uso de una variable Tipo tabla que me permita hacer mi procedimiento de inserción de mis datos a una variable temporal
        /// Para después hacer un left join, e insertar todo lo restante a la tabla de historicos
        /// </summary>
        /// <param name="con"></param>
        private static void usingTempVariableSQLTest(SqlConnection con)
        {
            string sql = "";

            //Agrego mi variable de Tabla Temporal a la sentencia SQL
            sql += @"DECLARE @VariableTabla TABLE (DN numeric(18,0), FECHA_ESTATUS date)";

            for (int i = 0; i < 50; i++)
            {
                sql += "INSERT INTO @VariableTabla VALUES (" + i + ",GETDATE())";
            }

            //sql += "SELECT * FROM @VariableTabla WHERE DN like '%1%'";
            sql += "SELECT * FROM [184301_ENVIO_MSJ_IVR_HISTORICO_DNS] A  " +
                " LEFT OUTER JOIN @VariableTabla B ON A.DN = @VariableTabla.DN WHERE A.DN != B.DN";

            SqlCommand cmd = new SqlCommand();

            cmd.Connection = con;
            cmd.CommandText = sql;

            using (DbDataReader reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        Console.WriteLine("DN: " + reader.GetValue(0) + " Fecha Estatus: " + reader.GetValue(1));
                    }
                }
            }

        }

        private static void CreateTempTable(SqlConnection con)
        {
            string sql = @"CREATE TABLE ##tempTableDNToday
                           (
                            DN   [numeric](18,0) NOT NULL INDEX ix1 NONCLUSTERED,
                            FECHA_ESTATUS  [date] NOT NULL,
                               
                           );";
            SqlCommand cmd = new SqlCommand();
            cmd.Connection = con;
            cmd.CommandText = sql;

            int rowCount = cmd.ExecuteNonQuery();

            Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());


        }

        private static void QueryDNs(SqlConnection con)
        {
            string sql = "Select * FROM[Prueba].[dbo].[184301_ENVIO_MSJ_IVR_HISTORICO_DNS]";

            //Create Command

            SqlCommand cmd = new SqlCommand();

            //connection for the command
            cmd.Connection = con;
            cmd.CommandText = sql;

            using (DbDataReader reader = cmd.ExecuteReader())
            {
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        //Get index of column DN in query statement
                        int DNIndex = reader.GetOrdinal("DN");

                        //Paso mi DN a un tipo de variable adecuado para imprimirlo
                        long DialNumber = Convert.ToInt64(reader.GetValue(0));

                        //paso mi fecha a un tipo de variable adecuada
                        DateTime fechaEstatus = reader.GetDateTime(1);

                        //Console.WriteLine("----------------");
                        //Console.Write("DN: "+DialNumber);
                        //Console.Write(" ");
                        //Console.Write("Fecha_Estatus: "+fechaEstatus);
                        //Console.WriteLine();

                        Console.WriteLine("DN: " + reader.GetValue(0)
                            + "  Fecha Estatus: " + reader.GetValue(1));
                    }
                }
            }
        }
    }
}
