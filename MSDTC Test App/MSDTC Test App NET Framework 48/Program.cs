using System;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using System.Transactions;

namespace MSDTCTestApp
{
    class Program
    {
        /// <summary>
        /// These properties should be provided in order to reproduce.
        /// </summary>
        static string connString = "**INSERT_CONNECTION_STRING_HERE**";
        private static void PopulateSqlCommand(SqlCommand command)
        {
            //Insert a value into a table.
            command.CommandText = "INSERT INTO MSDTC_TEST VALUES (@1)";
            command.Parameters.Add(new SqlParameter("@1", DateTime.UtcNow.ToString()));
        }
        static string tokenFile = @"c:\Temp\token.txt"; //File path for the transmitter propogation token.



        /// <summary>
        /// These properties reflect what we use in our production environment
        /// </summary>
        static TimeSpan transactionTimeout = TimeSpan.FromSeconds(60);
        static TransactionOptions transactionOptions = new TransactionOptions
        {
            Timeout = transactionTimeout,
            IsolationLevel = IsolationLevel.ReadUncommitted
        };
        static TransactionScopeAsyncFlowOption transactionScopeAsyncFlowOption = TransactionScopeAsyncFlowOption.Enabled;



        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello, World!");

            string transmitterPropogationToken = string.Empty;
            if (File.Exists(tokenFile))
            {
                transmitterPropogationToken = File.ReadAllText(tokenFile);
            }

            bool isInnerTransaction = false;
            if (!string.IsNullOrWhiteSpace(transmitterPropogationToken))
            {
                isInnerTransaction = true;
                Console.WriteLine($"Transmitter Propogation Token: {transmitterPropogationToken}");

                Transaction transaction = GetTransactionFromPropogationToken(transmitterPropogationToken);

                await StartTransactionAsync(transaction);
            }
            else
            {
                await StartTransactionAsync();
                await StartTransactionAsync();
            }

            if (isInnerTransaction)
            {
                Console.WriteLine("Transaction Disposed. Waiting for Outer Transaction to complete before exit");
            }
            else
            {
                Console.WriteLine("Transaction Disposed. Any Key to Exit");
            }
            Console.Read();
        }

        private static Transaction GetTransactionFromPropogationToken(string transmitterPropogationToken)
        {
            byte[] transactionToken = Convert.FromBase64String(transmitterPropogationToken);

            Transaction transaction = TransactionInterop.GetTransactionFromTransmitterPropagationToken(transactionToken);

            if (transaction != null)
            {
                Console.WriteLine($"Found Propogation Token");
            }
            else
            {
                throw new Exception("Unable to find Propogation Token");
            }

            return transaction;
        }

        private static async Task StartTransactionAsync(Transaction transaction = null)
        {
            Console.WriteLine("Starting New Transaction");

            try
            {
                TransactionScope transactionScope = transaction != null ?
                    new TransactionScope(transaction, transactionTimeout, transactionScopeAsyncFlowOption) :
                    new TransactionScope(TransactionScopeOption.Required, transactionOptions, transactionScopeAsyncFlowOption);

                using (transactionScope)
                {
                    Console.WriteLine("Transaction Started");

                    await PerformNonQuerySqlAsync();

                    if (transaction == null)
                    {
                        //If transaction is null then this is the outer transaction
                        await WaitForInnerTransaction();
                    }
                    else
                    {
                        //We are the inner transaction so we need to wait until the transaction is complete before exiting.
                        transaction.TransactionCompleted += (sender, e) => Environment.Exit(0);
                    }

                    transactionScope.Complete();

                    Console.WriteLine("Transaction Marked as Complete, disposing...");
                }
                Console.WriteLine("Transaction Disposed");
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
            finally
            {
                if (TokenFileExists())
                {
                    DeleteTokenFile();
                }
            }
        }

        private static async Task PerformNonQuerySqlAsync()
        {
            using (SqlConnection connection = new SqlConnection(connString))
            {
                await connection.OpenAsync();

                using (SqlCommand command = connection.CreateCommand())
                {
                    PopulateSqlCommand(command);

                    await command.ExecuteNonQueryAsync();
                }

                connection.Close();
            }

            Console.WriteLine("Inserted Value into Table");
        }

        private static async Task WaitForInnerTransaction()
        {
            //This is the outer transaction so let's get a propogation token.
            byte[] txPropogation = TransactionInterop.GetTransmitterPropagationToken(Transaction.Current);
            string token = Convert.ToBase64String(txPropogation);

            Console.WriteLine($"Writing Transmitter Propogation Token {token} to disk.");
            Console.WriteLine($"Start a new instance of this process to read token from disk and create transaction from it.");

            WritePropogationTokenToDisk(token);

            int seconds = 0;
            while (TokenFileExists() && seconds < 65)
            {
                Console.WriteLine($"Waiting for token file to be removed before committing transaction");
                await Task.Delay(TimeSpan.FromSeconds(5));
                seconds += 5;
            }
        }

        private static void DeleteTokenFile()
        {
            File.Delete(tokenFile);
        }

        private static bool TokenFileExists()
        {
            return File.Exists(tokenFile);
        }

        private static void WritePropogationTokenToDisk(string token)
        {
            File.WriteAllText(tokenFile, token);
        }
    }
}