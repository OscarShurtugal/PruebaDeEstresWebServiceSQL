using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Data;

namespace PruebaDeEstresWebServiceSQL
{
    
    class Program
    {
        static StreamWriter writer = new StreamWriter(@"C:\Users\oscarsanchez2\Desktop\pruebaThread.txt");

        static void Main(string[] args)
        {
            //procesoDB();

            Thread thread1;
            Thread thread2;
            Thread thread3;
            Thread thread4;

            thread1 = new Thread(new ThreadStart(BeginOperationThread1));
            thread2 = new Thread(new ThreadStart(BeginOperationThread2));
            thread3 = new Thread(new ThreadStart(BeginOperationThread3));
            thread4 = new Thread(new ThreadStart(BeginOperationThread4));
            thread1.IsBackground = true;
            thread2.IsBackground = true;
            thread3.IsBackground = true;
            thread4.IsBackground = true;
            thread1.Start();
            thread2.Start();
            thread3.Start();
            thread4.Start();
            Console.ReadLine();


            //procesoDBStoredProcedure();
        }

        private static void procesoDBStoredProcedure()
        {
            //int vIMSI_TEMP, vDN_TEMP;

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
                

                    SqlCommand cmd = new SqlCommand("SP_PruebaConsultaMultiple", con);
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.AddWithValue("@param", Convert.ToInt32(Textbox1.Text));

                    

                    cmd.Parameters.Add(new SqlParameter("@vDBVirtualMachineID", "Oscar")).Direction = ParameterDirection.Input;
                    int vDN_TEMP = 0;
                    cmd.Parameters.Add(new SqlParameter("@vDN_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;
                    int vIMSI_TEMP = 0;
                    cmd.Parameters.Add(new SqlParameter("@vIMSI_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();
                     vDN_TEMP=   Convert.ToInt32(cmd.Parameters["@vDN_TEMP"].Value);
                    vIMSI_TEMP = Convert.ToInt32(cmd.Parameters["@vIMSI_TEMP"].Value);
                    //Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
                    Console.WriteLine("Ejecutado: IMSI: " + vIMSI_TEMP + " DN: "+vDN_TEMP);
                    
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

            finally
            {
                con.Close();
            }

            //Console.ReadLine();
        }

        private static int procesoDBStoredProcedureThread1()
        {
            //int vIMSI_TEMP, vDN_TEMP;
            int vDN_TEMP = 0;
            int vIMSI_TEMP = 0;
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


                    SqlCommand cmd = new SqlCommand("SP_PruebaConsultaMultiple", con);
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.AddWithValue("@param", Convert.ToInt32(Textbox1.Text));



                    cmd.Parameters.Add(new SqlParameter("@vDBVirtualMachineID", "Thread 1")).Direction = ParameterDirection.Input;
                    
                    cmd.Parameters.Add(new SqlParameter("@vDN_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;
                  
                    cmd.Parameters.Add(new SqlParameter("@vIMSI_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();

                    if(!(cmd.Parameters["@vDN_TEMP"].Value is null))
                    vDN_TEMP = Convert.ToInt32(cmd.Parameters["@vDN_TEMP"].Value);
                    if (!(cmd.Parameters["@vIMSI_TEMP"].Value is null))

                        vIMSI_TEMP = Convert.ToInt32(cmd.Parameters["@vIMSI_TEMP"].Value);
                    //Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
                    Console.WriteLine("THREAD 1 Ejecutado: IMSI: " + vIMSI_TEMP + " DN: " + vDN_TEMP);

                    writeToFile(writer, vIMSI_TEMP, "Thread 1");
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

            finally
            {
                con.Close();
            }
            return vIMSI_TEMP;
            //Console.ReadLine();
        }


        private static int procesoDBStoredProcedureThread2()
        {
            //int vIMSI_TEMP, vDN_TEMP;
            int vDN_TEMP = 0;
            int vIMSI_TEMP = 0;
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


                    SqlCommand cmd = new SqlCommand("SP_PruebaConsultaMultiple", con);
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.AddWithValue("@param", Convert.ToInt32(Textbox1.Text));



                    cmd.Parameters.Add(new SqlParameter("@vDBVirtualMachineID", "Thread 2")).Direction = ParameterDirection.Input;
                
                    cmd.Parameters.Add(new SqlParameter("@vDN_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;
              
                    cmd.Parameters.Add(new SqlParameter("@vIMSI_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();
                    if (!(cmd.Parameters["@vDN_TEMP"].Value is null))
                    vDN_TEMP = Convert.ToInt32(cmd.Parameters["@vDN_TEMP"].Value);
                    if (!(cmd.Parameters["@vIMSI_TEMP"].Value is null))
                            
                    vIMSI_TEMP = Convert.ToInt32(cmd.Parameters["@vIMSI_TEMP"].Value);
                    //Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
                    Console.WriteLine("thread 2 Ejecutado: IMSI: " + vIMSI_TEMP + " DN: " + vDN_TEMP);

                    writeToFile(writer, vIMSI_TEMP, "Thread 2");
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

            finally
            {
                con.Close();
            }

            return vIMSI_TEMP;
            //Console.ReadLine();
        }


        private static int procesoDBStoredProcedureThread3()
        {
            //int vIMSI_TEMP, vDN_TEMP;
            int vDN_TEMP = 0;
            int vIMSI_TEMP = 0;
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


                    SqlCommand cmd = new SqlCommand("SP_PruebaConsultaMultiple", con);
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.AddWithValue("@param", Convert.ToInt32(Textbox1.Text));



                    cmd.Parameters.Add(new SqlParameter("@vDBVirtualMachineID", "Thread 3")).Direction = ParameterDirection.Input;
                
                    cmd.Parameters.Add(new SqlParameter("@vDN_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;
                  
                    cmd.Parameters.Add(new SqlParameter("@vIMSI_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();

                    if (!(cmd.Parameters["@vDN_TEMP"].Value is null))
                            vDN_TEMP = Convert.ToInt32(cmd.Parameters["@vDN_TEMP"].Value);
                        if (!(cmd.Parameters["@vIMSI_TEMP"].Value is null))

                    vIMSI_TEMP = Convert.ToInt32(cmd.Parameters["@vIMSI_TEMP"].Value);
                    //Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
                    Console.WriteLine("THREAD 3 Ejecutado: IMSI: " + vIMSI_TEMP + " DN: " + vDN_TEMP);

                    writeToFile(writer, vIMSI_TEMP, "Thread 3");
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

            finally
            {
                con.Close();
            }
            return vIMSI_TEMP;
            //Console.ReadLine();
        }


        private static int procesoDBStoredProcedureThread4()
        {
            //int vIMSI_TEMP, vDN_TEMP;
            int vDN_TEMP = 0;
            int vIMSI_TEMP = 0;
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

                    SqlCommand cmd = new SqlCommand("SP_PruebaConsultaMultiple", con);
                    cmd.CommandType = CommandType.StoredProcedure;

                    //cmd.Parameters.AddWithValue("@param", Convert.ToInt32(Textbox1.Text));



                    cmd.Parameters.Add(new SqlParameter("@vDBVirtualMachineID", "Thread 4")).Direction = ParameterDirection.Input;
                  
                    cmd.Parameters.Add(new SqlParameter("@vDN_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;
                    
                    cmd.Parameters.Add(new SqlParameter("@vIMSI_TEMP", SqlDbType.Int)).Direction = ParameterDirection.Output;

                    cmd.ExecuteNonQuery();

                    if (!(cmd.Parameters["@vDN_TEMP"].Value is null))
                            vDN_TEMP = Convert.ToInt32(cmd.Parameters["@vDN_TEMP"].Value);
                        if (!(cmd.Parameters["@vIMSI_TEMP"].Value is null))
                    vIMSI_TEMP = Convert.ToInt32(cmd.Parameters["@vIMSI_TEMP"].Value);
                    //Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
                    Console.WriteLine("thread 4 Ejecutado: IMSI: " + vIMSI_TEMP + " DN: " + vDN_TEMP);

                    writeToFile(writer, vIMSI_TEMP, "Thread 4");
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

            finally
            {
                con.Close();
            }
            return vIMSI_TEMP;
            //Console.ReadLine();
        }





        private static void procesoDB()
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
                    //Query a bd temporal
                    //usingTempVariableSQLTest(con);
                    insertIntoPrueba(con);
                    QueryaBDPrueba(con);
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


        private static void writeToFile(StreamWriter writer, int ent,string thread)
        {
            writer.WriteLine("Thread: " + thread + "    Numero: " + ent + "");
        }

        private static void BeginOperationThread4()
        {
            //for (int i = 0; i < 10000; i++)
            //{
            //    Console.WriteLine("Thread 4 ++++++++++++++ " + new Random().Next(1, 9000));
            //    //Thread.Sleep(new Random().Next(50,80));
            //    writeToFile(writer, i,"4");

            //}

            int vimsi = 0;
            do {
                vimsi = procesoDBStoredProcedureThread4();
                
            } while (vimsi != 0);
            

        }


        private static void BeginOperationThread3()
        {
            //    for (int i = 0; i < 10000; i++)
            //    {
            //        Console.WriteLine("Thread 3 ++++++++++++++ " + new Random().Next(1, 9000));
            //        //Thread.Sleep(new Random().Next(50,80));
            //        writeToFile(writer, i,"3");

            //    }


            int vimsi = 0;
            do
            {
                vimsi = procesoDBStoredProcedureThread3();

            } while (vimsi != 0);

        }


        private static void BeginOperationThread2()
        {
            //for (int i = 0; i < 10000; i++)
            //{
            //    Console.WriteLine("Thread 2 ++++++++++++++ " + new Random().Next(1,9000));
            //    //Thread.Sleep(new Random().Next(50,80));
            //    writeToFile(writer, i,"2");

            //}

            int vimsi = 0;
            do
            {
                vimsi = procesoDBStoredProcedureThread2();

            } while (vimsi != 0);
        }

        private static void BeginOperationThread1()
        {
            //for (int i = 0; i < 10000; i++)
            //{
            //    Console.WriteLine("Thread 1");
            //    writeToFile(writer, i,"1");
            //    //Thread.Sleep(new Random().Next(50, 80));
            //}
            int vimsi = 0;
            do
            {
                vimsi = procesoDBStoredProcedureThread1();

            } while (vimsi != 0);
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

            sql += "SELECT * FROM @VariableTabla";
            //sql += "SELECT * FROM @VariableTabla WHERE DN like '%1%'";
            //sql += "SELECT * FROM [184301_ENVIO_MSJ_IVR_HISTORICO_DNS] A  " +
            //    " LEFT OUTER JOIN @VariableTabla B ON A.DN = @VariableTabla.DN WHERE A.DN != B.DN";

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
            string sql = @"CREATE TABLE #tempTableDNHoy
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

        /// <summary>
        /// ABAJO COMIENZAN LOS METODOS ESPECIFICOS DE LA PRUEBA DE ESTRES
        /// </summary>
        /// <param name="con"></param>

        private static void QueryaBDPrueba(SqlConnection con)
        {
            string sql = "Select * FROM[Prueba].[dbo].[PruebaDeConsultaMultiple]";

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
                        Console.WriteLine("Folio: " + reader.GetValue(0) + " MSISDN: " + reader.GetValue(1)
                            + " IMSI_TEMP: " + reader.GetValue(2)
                            + " DN_TEMP: " + reader.GetValue(3)
                            + " R_WS_1: " + reader.GetValue(4)
                            + " R_WS_2: " + reader.GetValue(5)
                            + " R_WS_3: " + reader.GetValue(6)
                            + " MV: " + reader.GetValue(7)
                            + " ESTATUS_P: " + reader.GetValue(8)
                            + " FECHA_P: " + reader.GetValue(9));
                    }
                }
            }

        }

        private static void insertIntoPrueba(SqlConnection con)
        {
            string sql = "";

            //Agrego mi variable de Tabla Temporal a la sentencia SQL
            //sql += @"DECLARE @VariableTabla TABLE (DN numeric(18,0), FECHA_ESTATUS date)";
            SqlCommand cmd = new SqlCommand();

            cmd.Connection = con;
            for (int i = 0; i < 1000000; i++)
            {

                //insert into PruebaDeConsultaMultiple(folio, MSISDN, IMSI_TEMP, MSISDN_TEMP) values(3, 3, 3, 3)
                //sql += "INSERT INTO PruebaDeConsultaMultiple(folio, MSISDN, IMSI_TEMP, MSISDN_TEMP) VALUES (" + i + "," + i + "," + i + "," + i + ")";
                sql = "INSERT INTO PruebaDeConsultaMultiple(folio, MSISDN, IMSI_TEMP, MSISDN_TEMP) VALUES (" + i + "," + i + "," + i + "," + i + ")";
                Console.WriteLine(i);

                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
            cmd.CommandText = sql;



            Console.WriteLine("Rows Affected: " + cmd.ExecuteNonQuery());
    

        }

    }
}
