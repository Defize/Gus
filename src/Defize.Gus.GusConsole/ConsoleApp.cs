namespace Defize.Gus.GusConsole
{
    using System;
    using CLAP;
    using CLAP.Validation;

    internal class ConsoleApp
    {
        [Verb(Aliases = "a", Description = "Apply the specifed SQL scripts to the specified database.")]
        public static void Apply(
            [Aliases("src")] [Description("The source folder.")]
            [DirectoryExists]
            string source,
            [Aliases("svr")] [Description("The destination server.")] [Required]
            string server,
            [Aliases("db")] [Description("The name of the database.")] [Required]
            string database,
            [Aliases("cd")] [Description("Creates the database if missing.")] [DefaultValue(false)]
            bool createDatabaseIfMissing,
            [Aliases("cms")] [Description("Creates the Gus schema if missing.")] [DefaultValue(true)]
            bool createManagementSchemaIfMissing,
            [Aliases("ro")] [Description("Register scripts without executing.")] [DefaultValue(false)]
            bool recordOnly,
            [Aliases("hoe")] [Description("Stop processing scripts when there is an error")] [DefaultValue(true)]
            bool haltOnError,
            [Aliases("usr")] [Description("Username for the server.")] 
            string username,
            [Aliases("pw")] [Description("Password.")] 
            string password)
        {
            var configuration = new ApplyTaskConfiguration
                                    {
                                        Server = server,
                                        Database = database,
                                        CreateDatabaseIfMissing = createDatabaseIfMissing,
                                        CreateManagementSchemaIfMissing = createManagementSchemaIfMissing,
                                        RecordOnly = recordOnly,
                                        SourcePath = source,
                                        HaltOnError = haltOnError,
                                        UserName = username,
                                        Password = password
                                    };

            var context = new GusTaskExecutionContext();
            context.ExecutionEvent += TaskExecutionEventHandler;
            var task = new ApplyTask();

            try
            {
                var success = task.Execute(configuration, context);

                if (success)
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Success, "Task completed successfully.", 0));
                }
                else
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Error, "Task completed with errors.", 0));
                }
            }
            finally
            {
                context.ExecutionEvent -= TaskExecutionEventHandler;
            }
        }

        [Verb(Aliases = "s", Description = "List the SQL scripts not yet applied to the specified database.")]
        public static void Status(
            [Aliases("src")] [Description("The source folder.")] [DirectoryExists]
            string source,
            [Aliases("svr")] [Description("The destination server.")][Required]
            string server,
            [Aliases("db")] [Description("The name of the database.")][Required]
            string database,
            [Aliases("usr")] [Description("Username for the server.")]
            string username,
            [Aliases("pw")] [Description("Password.")]
            string password)
        {
            var configuration = new StatusTaskConfiguration
            {
                Server = server,
                Database = database,
                SourcePath = source,
                UserName = username,
                Password = password
            };

            var context = new GusTaskExecutionContext();
            context.ExecutionEvent += TaskExecutionEventHandler;
            var task = new StatusTask();

            try
            {
                var success = task.Execute(configuration, context);

                if (success)
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Success, "Task completed successfully.", 0));
                }
                else
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Error, "Task completed with errors.", 0));
                }
            }
            finally
            {
                context.ExecutionEvent -= TaskExecutionEventHandler;
            }
        }

        [Verb(Aliases = "c", Description = "Create a new SQL script with a unique timestamped name.")]
        public static void Create(
            [Aliases("n")][Description("The name of the script to create.")][Required]
            string name)
        {
            var configuration = new CreateTaskConfiguration
            {
                Name = name
            };

            var context = new GusTaskExecutionContext();
            context.ExecutionEvent += TaskExecutionEventHandler;
            var task = new CreateTask();

            try
            {
                var success = task.Execute(configuration, context);

                if (success)
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Success, "Task completed successfully.", 0));
                }
                else
                {
                    DisplayExecutionEvent(new GusTaskExecutionEventArgs(ExecutionEventType.Error, "Task completed with errors.", 0));
                }
            }
            finally
            {
                context.ExecutionEvent -= TaskExecutionEventHandler;
            }
        }

        [Verb(Aliases = "v", Description = "Verify the specified SQL scripts against the specified database.")]
        public static void Verify()
        {

        }

        [Empty]
        [Help(Aliases = "h,?")]
        public static void Help(string help)
        {
            Console.WriteLine(help);
        }

        [Error]
        public static void Error(ExceptionContext ex)
        {
            Console.WriteLine("The horrors.");
            Console.WriteLine(ex.Exception.Message);
            Console.WriteLine(ex.Exception.StackTrace);
        }

        private static void TaskExecutionEventHandler(object sender, GusTaskExecutionEventArgs e)
        {
            DisplayExecutionEvent(e);
        }

        private static void DisplayExecutionEvent(GusTaskExecutionEventArgs e)
        {
            ConsoleHelper.WriteExecutionEventToConsole(e);
        }
    }
}
