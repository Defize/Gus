﻿namespace Defize.Gus
{
    public class ApplyTaskConfiguration
    {
        public string Server { get; set; }
        public string Database { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public bool CreateDatabaseIfMissing { get; set; }
        public bool CreateManagementSchemaIfMissing { get; set; }
        public bool RecordOnly { get; set; }
        public string SourcePath { get; set; }
        public bool HaltOnError { get; set; }
    }
}
