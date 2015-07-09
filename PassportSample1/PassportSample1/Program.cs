using System;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading;

namespace PassportSample1
{
    class Program
    {
        static void DownLoadZip()
        {
            using (var client = new WebClient())
            {
                try
                {
                    client.DownloadFile("http://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Provider-of-Services/Downloads/MAR15_OTHER_CSV.zip", AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip");
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        static void UnZipDownload()
        {
            try
            {
                ZipFile.ExtractToDirectory(AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip", AppDomain.CurrentDomain.BaseDirectory + @"Processing\");
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void LoadDownloadCSV()
        {
            string connStr = @"Server=LISA-DT\SQLEXPRESS;Database=Passport;Trusted_Connection=true";

            try
            {
                SqlConnection conn = new SqlConnection(connStr);
                SqlCommand sqlcmd = new SqlCommand();
                sqlcmd.Connection = conn;

                conn.Open();

                try
                {
                    /*
                    sqlcmd.CommandText = @"BEGIN TRANSACTION
                    BEGIN TRY
                    BULK INSERT dbo.CSV_Temp_Table
                    FROM '" + AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_OTHER_MAR15.csv'
                    WITH
                    (
                        FIELDTERMINATOR = ',',
                        ROWTERMINATOR = '\n',
                        ROWS_PER_BATCH = 10000, 
                        TABLOCK
                    )
                    COMMIT TRANSACTION
                    END TRY
                    BEGIN CATCH
                    ROLLBACK TRANSACTION
                    END CATCH";
                    */

                    sqlcmd.CommandText = @"
                    BULK INSERT dbo.CSV_Temp_Table
                    FROM '" + AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_OTHER_MAR15.csv'
                    WITH
                    (
                        FIRSTROW = 2,
                        FIELDTERMINATOR = ',',
                        ROWS_PER_BATCH = 10000, 
                        TABLOCK
                    )";

                    sqlcmd.ExecuteNonQuery();

                    Directory.Delete(AppDomain.CurrentDomain.BaseDirectory + @"Processing\", true);
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    conn.Close();
                }
      
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        static void Main(string[] args)
        {
            string[] CurArr = new string[4];
            int idx = 0;

            CurArr[0] = @"\";
            CurArr[1] = "|";
            CurArr[2] = "/";
            CurArr[3] = "-";

            int ProcessID = 0;

            string[] Operation = new string[3];
            Operation[0] = "Downloading zip file";
            Operation[1] = "Unzipping download";
            Operation[2] = "Loading Database with CSV file";

            ThreadStart[] ThreadFunction = new ThreadStart[3];
            ThreadFunction[0] = new ThreadStart(DownLoadZip);
            ThreadFunction[1] = new ThreadStart(UnZipDownload);
            ThreadFunction[2] = new ThreadStart(LoadDownloadCSV);

            Thread[] WorkerThread = new Thread[3];
            WorkerThread[0] = new Thread(ThreadFunction[0]);
            WorkerThread[1] = new Thread(ThreadFunction[1]);
            WorkerThread[2] = new Thread(ThreadFunction[2]);

            if(Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + @"\Processing\") == false)
            {
                Directory.CreateDirectory(AppDomain.CurrentDomain.BaseDirectory + @"\Processing\");
            }

            while (ProcessID < 3)
            {
                if (!WorkerThread[ProcessID].IsAlive) { WorkerThread[ProcessID].Start(); }

                Console.Write(Operation[ProcessID] + " - press Esc to terminate current operation ");

                while (WorkerThread[ProcessID].ThreadState == ThreadState.Running)
                {
                    if (Console.KeyAvailable)
                    {
                        if (Console.ReadKey(true).Key == ConsoleKey.Escape)
                        {
                            Console.WriteLine(" ");
                            Console.WriteLine("Please wait... aborting [" + Operation[ProcessID] + "]");
                            WorkerThread[ProcessID].Abort();
                            return;
                        }
                    }
                    else
                    {
                        Console.Write(CurArr[idx++]);
                        Console.SetCursorPosition(Console.CursorLeft - 1, Console.CursorTop);
                        idx %= 4;
                        Thread.Sleep(55);
                    }
                }

                Console.WriteLine(" ");
                switch (ProcessID)
                {
                    case 0:
                        if (File.Exists(AppDomain.CurrentDomain.BaseDirectory + @"Processing\POS_Download.zip"))
                        {
                            Console.WriteLine("Download complete");
                        }
                        else
                        {
                            Console.WriteLine("Download failed");
                            return;
                        }
                        break;
                    case 1:
                        if (Directory.GetFiles(AppDomain.CurrentDomain.BaseDirectory + @"Processing\", "*.csv").Length > 0)
                        {
                            Console.WriteLine("Unzipping completed");
                        }
                        else
                        {
                            Console.WriteLine("unzip failed");
                            return;
                        }
                        break;
                    case 2:
                        if (!Directory.Exists(AppDomain.CurrentDomain.BaseDirectory + @"Processing\"))
                        {
                            Console.WriteLine("Loading database with CSV file completed");
                        }
                        else
                        {
                            Console.WriteLine("Loading database with CSV file failed");
                            return;
                        }
                        break;
                }

                ProcessID++;
            }

            Console.WriteLine("Operations completed... press any key to continue");
            Console.ReadKey();
        }
    }
}
