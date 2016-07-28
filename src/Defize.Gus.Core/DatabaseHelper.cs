namespace Defize.Gus
{
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using Microsoft.SqlServer.Management.Smo;

    internal class DatabaseHelper
    {
        private const string SchemaName = "Gus";
        private const string HistoryTableName = "GusHistory";
        private string HistoryTablePrimaryKeyName = $"PK_{SchemaName}_{HistoryTableName}";
        private const string HistoryTableFilenameColumnName = "Filename";
        private const string HistoryTableHashColumnName = "Hash";
        private const string HistoryTableAppliedOnColumnName = "AppliedOn";
        private const string HistoryTableAppliedByColumnName = "AppliedBy";
        private string RegisterSqlScript = "INSERT INTO [{0}].[{1}] ([Filename], [Hash], [AppliedOn], [AppliedBy]) VALUES (N'{2}', N'{3}', GETDATE(), SYSTEM_USER)";        

        private readonly GusTaskExecutionContext _context;

        public DatabaseHelper(GusTaskExecutionContext context)
        {
            _context = context;
        }

        public SqlConnection CreateAndOpenConnection(string server, string database, string username, string password, bool createDatabaseIfMissing)
        {
            _context.RaiseExecutionEvent(string.Format("Connecting to database '{0}'.", server));

            var connectionBuilder = new SqlConnectionStringBuilder { DataSource = server, IntegratedSecurity = true };

            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            {
                connectionBuilder.IntegratedSecurity = false;
                connectionBuilder.UserID = username;
                connectionBuilder.Password = password;
                connectionBuilder.Encrypt = true;
            }

            if (!createDatabaseIfMissing)
            {
                connectionBuilder.InitialCatalog = database;
            }

            var connectionString = connectionBuilder.ToString();

            var connection = new SqlConnection(connectionString);

            return Connect(connection);
        }

        private SqlConnection Connect(SqlConnection connection)
        {
            try
            {

                connection.Open();
            }
            catch (SqlException ex)
            {
                _context.RaiseExecutionEvent(ExecutionEventType.Error, string.Format("Unable to open connection to database: {0}", ex.Message));
                return null;
            }

            return connection;
        }

        public Database InitializeDatabase(Server server, string databaseName, bool createDatabaseIfMissing, bool createManagementSchemaIfMissing)
        {
            _context.RaiseExecutionEvent("Initialising database.");

            var database = OpenDatabase(server, databaseName, createDatabaseIfMissing);
            if (database == null)
            {
                return null;
            }

            var isSchemaValid = CheckSchema(server, createManagementSchemaIfMissing);
            if (!isSchemaValid)
            {
                return null;
            }

            var isHistoryTableValid = CheckHistoryTable(server, databaseName, createManagementSchemaIfMissing);
            if (!isHistoryTableValid)
            {
                return null;
            }

            return database;
        }

        public ICollection<AppliedScript> GetPreviouslyAppliedScripts(Server server)
        {
            var results = server.ConnectionContext.ExecuteWithResults($"SELECT [Filename], [Hash] FROM [{SchemaName}].[{HistoryTableName}]");
            var table = results.Tables[0];
            var rows = table.Rows;

            return rows.Cast<DataRow>().Select(r => new AppliedScript
                                                           {
                                                               Filename = (string)r[HistoryTableFilenameColumnName],
                                                               Hash = (string)r[HistoryTableHashColumnName]
                                                           }).ToList();
        }

        public void RecordScript(Server server, string filename, string hash)
        {
            filename = filename.Replace("'", "''");
            var sql = string.Format(RegisterSqlScript, SchemaName, HistoryTableName, filename, hash);
            server.ConnectionContext.ExecuteNonQuery(sql);
        }

        private Database OpenDatabase(Server server, string databaseName, bool createDatabaseIfMissing)
        {
            var databases = server.Databases.Cast<Database>().ToList();

            var database = server.Databases[databaseName];

            if (database == null)
            {
                if (createDatabaseIfMissing)
                {
                    database = new Database(server, databaseName);
                    database.Create();
                }
                else
                {
                    _context.RaiseExecutionEvent(ExecutionEventType.Error, string.Format("The database '{0}' could not be found.", databaseName));
                    return null;
                }
            }

            return database;
        }

        private bool CheckSchema(Server server, bool createManagementSchemaIfMissing)
        {

            var schemas = server.ConnectionContext.ExecuteWithResults("SELECT name FROM sys.schemas").Tables[0].Rows.Cast<DataRow>();

            var schema = schemas.FirstOrDefault(x => x.ItemArray[0].ToString() == SchemaName);

            if (schema == null)
            {
                if (createManagementSchemaIfMissing)
                {
                    server.ConnectionContext.ExecuteNonQuery($"CREATE SCHEMA [{SchemaName}]");
                }
                else
                {
                    _context.RaiseExecutionEvent(ExecutionEventType.Error, "The Gus management schema could not be found.");
                    return false;
                }
            }

            return true;
        }

        private bool CheckHistoryTable(Server server, string databaseName, bool createManagementSchemaIfMissing)
        {
            var results = server.ConnectionContext.ExecuteWithResults($@"SELECT * FROM INFORMATION_SCHEMA.TABLES 
                                                                       WHERE TABLE_TYPE = 'BASE TABLE' 
                                                                       AND TABLE_CATALOG = '{databaseName}' 
                                                                       AND TABLE_SCHEMA = '{SchemaName}' 
                                                                       AND TABLE_NAME = '{HistoryTableName}'").Tables[0].Rows.Cast<DataRow>();

            if (!results.Any())
            {
                if (createManagementSchemaIfMissing)
                {
                    server.ConnectionContext.ExecuteNonQuery($@"CREATE TABLE [{SchemaName}].[{HistoryTableName}](
                                                                [Filename][nvarchar](256) NOT NULL,
                                                                [Hash][nvarchar](128) NOT NULL,
                                                                [AppliedOn][datetime] NOT NULL,
                                                                [AppliedBy][nvarchar](256) NOT NULL,
                                                                 CONSTRAINT [{HistoryTablePrimaryKeyName}] PRIMARY KEY CLUSTERED 
                                                                (
	                                                                [Filename] ASC
                                                                )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                                                                ) ON [PRIMARY]");
                }
                else
                {
                    _context.RaiseExecutionEvent(ExecutionEventType.Error, "The Gus management schema is invalid.");
                    return false;
                }
            }

            return true;
        }
    }
}
