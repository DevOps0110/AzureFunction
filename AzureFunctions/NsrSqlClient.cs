using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;

namespace NsrFunctions
{
    class NsrSqlClient : IDisposable
    {
        private readonly SqlConnection _configurationDatabaseConnection;

        public NsrSqlClient()
        {
            _configurationDatabaseConnection = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnection"));
            _configurationDatabaseConnection.Open();
        }

        public string GetSourceSysIdForFileType(string fileType, string containerName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {   
                cmd.Command.CommandText = @"SELECT src_sys_id from t_cnfg_src_sys where file_nm_ptrn = (@fileType) and file_type like '%bottler%'";
                
                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = $@"{containerName}_yyyymmdd_hhmmss_{fileType}.csv";
                param.ParameterName = @"fileType";
                cmd.Command.Parameters.Add(param);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new KeyNotFoundException($@"No Source Sys Id was found in t_cnfg_src_sys table for bottler currency file - [{fileType}]");
            }
        }

        public string GetFactTypeForSrcId(string sourceSysId)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"select top 1 b.fact_type from t_src_sys a inner join t_dmm_module b on a.module_id = b.module_id where a.src_sys_id = (@sourceSysId) ";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new ArgumentException($@"No fact type found for srcSysId {sourceSysId} in t_dmm_module or t_src_sys table", nameof(sourceSysId));
            }
        }

        public string GetModuleIdForSrcId(string sourceSysId)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"select top 1 module_id from t_src_sys where src_sys_id = (@sourceSysId) ";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new ArgumentException($@"No Module ID found for srcSysId {sourceSysId} in t_src_sys table", nameof(sourceSysId));
            }
        }

        public void UpdateStatusForBottlerFileForAutoCurrencyFiles(string sourceSysId, string fileName, string fileLoadStatus, string fileSetId)
        {

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"UPDATE t_audit_file_sub SET file_load_status = @fileLoadStatus, catalog_dttm = getdate() 
                                            WHERE src_sys_id = @sourceSysId and src_file_nm = @fileName and file_set_id = @fileSetId ";
                
                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;                           //Source system id of the submission entity from t_src_sys table
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;                                 //Source file name with extension
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var fileLoadStatusParam = cmd.Command.CreateParameter();
                fileLoadStatusParam.DbType = System.Data.DbType.String;
                fileLoadStatusParam.Value = fileLoadStatus;                          //AF-READY
                fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                cmd.Command.Parameters.Add(fileLoadStatusParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = fileSetId;                          //AF-READY
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                cmd.Command.ExecuteNonQuery();
            }
        }

        public string GetLCFCnfgForNonNSRFile(string bottlerPath)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT * FROM dbo.t_src_sys_nsr_file_cnvr WHERE src_azr_blob_cntr = (@bottlerPath)"; // AND src_grp_id = 2";

                // we know the db has all the paths end in '/' so if our path parameter doesn't, add it on
                // Preferably the db wouldn't be using paths as lookup values, but this is what we have for now
                if (!bottlerPath.EndsWith("/")) bottlerPath += "/";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = bottlerPath;
                param.ParameterName = @"bottlerPath";

                cmd.Command.Parameters.Add(param);

                SqlDataAdapter adpt = new SqlDataAdapter(cmd.Command);
                DataTable dt = new DataTable();
                adpt.Fill(dt);

                if (dt.Columns.Contains("lcf_ind"))
                    return dt.Rows[0]["lcf_ind"].ToString();
                else
                    return "False";
            }
        }

        public string GetShipFromConfig(string sourceSysId)
        {
            var allRecords = new List<NsrFileCnvrDataRow>();

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @" SELECT [file_nm_ptrn]                                                 
                                              FROM [dbo].[t_cnfg_src_sys_nsr_file_cnvr]
                                            WHERE [src_sys_id] = (@sourceSysId) and file_mask = 'shipfrom' order by file_nm_ptrn desc"; // AND src_grp_id = 2";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = sourceSysId;
                param.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(param);

                return cmd.Command.ExecuteScalar()?.ToString();
            }
        }

        public IEnumerable<NsrFileCnvrDataRow> GetFilesRequiredForConversion(string sourceSysId)
        {
            var allRecords = new List<NsrFileCnvrDataRow>();

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @" SELECT [cnfg_src_sys_infa_id]      
                                                  ,[file_seq_id]
                                                  ,[file_set_id]
                                                  ,[file_set_sub_id]
                                                  ,[max_file_set_sub_id]
                                                  ,[file_mask]
                                                  ,[file_nm_ptrn]
                                                  ,[is_file_south_latin]
                                                  ,[has_filename_datetime]
                                              FROM [dbo].[t_cnfg_src_sys_nsr_file_cnvr]
                                            WHERE [src_sys_id] = (@sourceSysId) order by file_nm_ptrn desc"; // AND src_grp_id = 2";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = sourceSysId;
                param.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(param);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var newRow = new NsrFileCnvrDataRow
                            {
                                CnfgSrcSysInfaId = reader.GetInt32(0),
                                FileSeqId = reader.GetInt32(1),
                                FileSetId = reader.GetInt32(2),
                                FileSetSubId = reader.GetInt32(3),
                                MaxFileSetSubId = reader.GetInt32(4),
                                FileMask = reader[5].ToString(),
                                FileNamePattern = reader[6].ToString(),
                                IsSouthLatinFile = reader.IsDBNull(7) ? false : reader.GetBoolean(7),
                                HasDatetimeFormat = reader.IsDBNull(8) ? true : reader.GetBoolean(8)

                            };

                            yield return newRow;
                        }
                    }
                }
            }
        }

        public IEnumerable<string> GetExpectedFilesForTarget(string sourceSysId, string factType)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_mask FROM dbo.t_cnfg_tgt_sys_nsr_file_cnvr WHERE src_sys_id = (@sourceSysId)";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = sourceSysId;
                param.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(param);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var value = reader.GetString(0);
                            if (value.Contains(factType, StringComparison.OrdinalIgnoreCase))
                            {
                                yield return $@"{value}.csv";
                            }
                        }
                    }
                }
            }
        }

        public NsrFileCnvrDataRow GetConfigurationRecord(string sourceSysId)
        {
            NsrFileCnvrDataRow curRow = new NsrFileCnvrDataRow();
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @" SELECT top 1 [cnfg_src_sys_infa_id]      
                                                  ,[file_seq_id]
                                                  ,[file_set_id]
                                                  ,[file_set_sub_id]
                                                  ,[max_file_set_sub_id]
                                                  ,[file_mask]
                                                  ,[file_nm_ptrn]
                                                  
                                              FROM [dbo].[t_cnfg_src_sys_nsr_file_cnvr]
                                            WHERE [src_sys_id] = (@sourceSysId) order by file_nm_ptrn desc";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = sourceSysId;
                param.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(param);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            var newRow = new NsrFileCnvrDataRow
                            {
                                CnfgSrcSysInfaId = reader.GetInt32(0),
                                FileSeqId = reader.GetInt32(1),
                                FileSetId = reader.GetInt32(2),
                                FileSetSubId = reader.GetInt32(3),
                                MaxFileSetSubId = reader.GetInt32(4),
                                FileMask = reader[5].ToString(),
                                FileNamePattern = reader[6].ToString(),
                            };
                            return newRow;
                        }
                    }
                }
            }
            return curRow;
        }
        public IEnumerable<(int InitialCharacterPosition, int DataLength)> GetPositionalElementsForLatinFileConversion(int sourceSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT intial_char, data_length 
                                            FROM dbo.t_cnfg_file_latam_nsr_file_cnvr 
                                            WHERE intial_char IS NOT NULL AND file_nm = (@fileName)
                                            AND src_sys_id = (@sourceSysId) ORDER BY col_seq_no";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            yield return (reader.GetInt32(0),
                                int.Parse(reader.GetString(1)));  // due to NSRP-4115
                        }
                    }
                }
            }
        }

        public string GetSourceSysIdForNonNSRFile(string bottlerPath)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT src_sys_id FROM dbo.t_src_sys_nsr_file_cnvr WHERE src_azr_blob_cntr = (@bottlerPath)"; // AND src_grp_id = 2";

                // we know the db has all the paths end in '/' so if our path parameter doesn't, add it on
                // Preferably the db wouldn't be using paths as lookup values, but this is what we have for now
                if (!bottlerPath.EndsWith("/")) bottlerPath += "/";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = bottlerPath;
                param.ParameterName = @"bottlerPath";

                cmd.Command.Parameters.Add(param);

                return cmd.Command.ExecuteScalar()?.ToString();
            }
        }

        public int CheckRowId(string sourceSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT row_id FROM dbo.t_azure_audit_nsr_file_cnvr WHERE src_sys_id = (@sourceSysId) and src_file_nm = (@fileName) order by row_id desc";

                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                return (int)(cmd.Command.ExecuteScalar() ?? 0);
            }
        }

        public void InsertForNSRFileReceived(string sourceSysId, NsrFileCnvrDataRow currentRecord, string fileName, string fileLoadStatus, Int64 fileNumId = 0)
        {
            //var rowId = CheckRowId(sourceSysId, fileName);

            //if (rowId <= 0)
            //{
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"INSERT INTO t_azure_audit_nsr_file_cnvr (src_sys_id, cnfg_src_sys_infa_id, file_seq_id, file_set_id, file_set_sub_id, 
                                                    max_file_set_sub_id, src_file_nm, file_load_status, catalog_dttm, file_num_id) 
                                                VALUES (@sourceSysId, @infaParamSetId, @fileSeqId, @fileSetId, @fileSetSubId,
                                                    @maxFileSetSubId, @fileName, @fileLoadStatus, @recordTime, @fileNumId)";

                //file_id – auto increment for each new entry
                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var infaParamSetIdParam = cmd.Command.CreateParameter();
                infaParamSetIdParam.DbType = System.Data.DbType.String;
                infaParamSetIdParam.Value = currentRecord.CnfgSrcSysInfaId;
                infaParamSetIdParam.ParameterName = @"infaParamSetId";
                cmd.Command.Parameters.Add(infaParamSetIdParam);

                var fileSeqIdParam = cmd.Command.CreateParameter();
                fileSeqIdParam.DbType = System.Data.DbType.String;
                fileSeqIdParam.Value = currentRecord.FileSeqId;
                fileSeqIdParam.ParameterName = @"fileSeqId";
                cmd.Command.Parameters.Add(fileSeqIdParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = currentRecord.FileSetId;
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                var fileSetSubIdParam = cmd.Command.CreateParameter();
                fileSetSubIdParam.DbType = System.Data.DbType.String;
                fileSetSubIdParam.Value = currentRecord.FileSetSubId;
                fileSetSubIdParam.ParameterName = @"fileSetSubId";
                cmd.Command.Parameters.Add(fileSetSubIdParam);

                var maxFileSetSubIdParam = cmd.Command.CreateParameter();
                maxFileSetSubIdParam.DbType = System.Data.DbType.String;
                maxFileSetSubIdParam.Value = currentRecord.MaxFileSetSubId;
                maxFileSetSubIdParam.ParameterName = @"maxFileSetSubId";
                cmd.Command.Parameters.Add(maxFileSetSubIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var fileLoadStatusParam = cmd.Command.CreateParameter();
                fileLoadStatusParam.DbType = System.Data.DbType.String;
                fileLoadStatusParam.Value = fileLoadStatus;
                fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                cmd.Command.Parameters.Add(fileLoadStatusParam);

                var recordTimeParam = cmd.Command.CreateParameter();
                recordTimeParam.DbType = System.Data.DbType.DateTime;
                recordTimeParam.Value = System.DateTime.UtcNow;
                recordTimeParam.ParameterName = @"recordTime";
                cmd.Command.Parameters.Add(recordTimeParam);

                var fileNumIdParam = cmd.Command.CreateParameter();
                fileNumIdParam.DbType = System.Data.DbType.Int64;
                fileNumIdParam.Value = fileNumId;
                fileNumIdParam.ParameterName = @"fileNumId";
                cmd.Command.Parameters.Add(fileNumIdParam);

                cmd.Command.ExecuteNonQuery();
            }
            //}
        }

        public int GetFileSetId()
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandTimeout = 0;
                cmd.Command.CommandText = @"nextval";                                   // Fetch fileset id
                cmd.Command.CommandType = System.Data.CommandType.StoredProcedure;

                //@sequence_name = t_audit_file_sub     - in (string)
                //@cur_val                              - out (bigint)

                var sequenceNamaParam = cmd.Command.CreateParameter();
                sequenceNamaParam.DbType = System.Data.DbType.String;
                sequenceNamaParam.Value = "t_audit_file_sub";
                sequenceNamaParam.ParameterName = @"sequence_name";
                cmd.Command.Parameters.Add(sequenceNamaParam);

                var outParam = cmd.Command.CreateParameter();
                outParam.DbType = System.Data.DbType.Int32;
                outParam.Direction = System.Data.ParameterDirection.InputOutput;
                outParam.ParameterName = @"cur_val";
                outParam.Value = 0;
                cmd.Command.Parameters.Add(outParam);

                cmd.Command.ExecuteNonQuery();

                var fileSetId = Convert.ToInt32(outParam.Value);
                return fileSetId;
            }
        }

        public Int64 GetFileNumIdForFileConversion()
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandTimeout = 0;
                cmd.Command.CommandText = @"nextval_nsr_file_cnvr";                                   // Fetch fileset id
                cmd.Command.CommandType = System.Data.CommandType.StoredProcedure;

                //@sequence_name = t_audit_file_sub     - in (string)
                //@cur_val                              - out (bigint)

                var sequenceNamaParam = cmd.Command.CreateParameter();
                sequenceNamaParam.DbType = System.Data.DbType.String;
                sequenceNamaParam.Value = "t_azure_file_num_nsr_file_cnvr";
                sequenceNamaParam.ParameterName = @"sequence_name";
                cmd.Command.Parameters.Add(sequenceNamaParam);

                var outParam = cmd.Command.CreateParameter();
                outParam.DbType = System.Data.DbType.Int64;
                outParam.Direction = System.Data.ParameterDirection.InputOutput;
                outParam.ParameterName = @"cur_val";
                outParam.Value = 0;
                cmd.Command.Parameters.Add(outParam);

                cmd.Command.ExecuteNonQuery();

                var fileSetId = Convert.ToInt64(outParam.Value);
                return fileSetId;
            }
        }

        public string GetFileSetId(string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_set_id FROM dbo.t_audit_file_sub 
                                                WHERE src_file_nm = (@fileName) and file_load_status = 'AF-READY' order by file_id desc";

                cmd.Command.CommandTimeout = 0;

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var setId = cmd.Command.ExecuteScalar()?.ToString() ?? "0";
                return setId;
            }
        }

        public int DeleteInvalidData(string sourceSysId, string fileSetId)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"DELETE FROM dbo.t_audit_file_sub WHERE src_sys_id = (@sourceSysId) and src_file_nm = (@fileSetId)";

                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = fileSetId;
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                return cmd.Command.ExecuteNonQuery();
            }
        }

        public string GetFactTypeForFile(string srcSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT TOP 1 fact_type FROM t_audit_file_sub where src_sys_id = (@srcSysId) AND src_file_nm = (@fileName) ORDER BY file_id DESC";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = srcSysId;
                sourceSysIdParam.ParameterName = @"srcSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var filenameParam = cmd.Command.CreateParameter();
                filenameParam.DbType = System.Data.DbType.String;
                filenameParam.Value = fileName;
                filenameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(filenameParam);

                var retVal = cmd.Command.ExecuteScalar();
                if (retVal == null) throw new KeyNotFoundException($@"No fact type id has been inserted in to the audit table for srcSysId {srcSysId} and src_file_nm {fileName}");

                return Convert.ToString(retVal);
            }

        }

        public int GetModuleIdForFile(string srcSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT TOP 1 module_id FROM t_audit_file_sub where src_sys_id = (@srcSysId) AND src_file_nm = (@fileName) ORDER BY file_set_id DESC";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = srcSysId;
                sourceSysIdParam.ParameterName = @"srcSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var filenameParam = cmd.Command.CreateParameter();
                filenameParam.DbType = System.Data.DbType.String;
                filenameParam.Value = fileName;
                filenameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(filenameParam);

                var retVal = cmd.Command.ExecuteScalar();
                if (retVal == null) throw new KeyNotFoundException($@"No module id has been inserted in to the audit table for srcSysId {srcSysId} and src_file_nm {fileName}");

                return Convert.ToInt32(retVal);
            }

        }

        public int GetProductAuditTableRowId(string sourceSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_id FROM dbo.t_audit_file_sub WHERE src_sys_id = (@sourceSysId) and src_file_nm = (@fileName) order by file_id desc ";

                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                return (int)(cmd.Command.ExecuteScalar() ?? 0);
            }
        }

        public DateTime? GetNSRFileCreateDateTime(string sourceSysId, NsrFileCnvrDataRow currentRecord, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT src_file_create_dttm
                                                    FROM t_azure_audit_nsr_file_cnvr 
				                                    WHERE src_sys_id=@sourceSysId AND src_file_nm=@fileName order by 1 desc ";

                //file_id – auto increment for each new entry
                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var reader = cmd.Command.ExecuteReader();
                if (reader.HasRows && reader.Read())
                {
                    return Convert.ToDateTime(reader[0]);
                }

                return null;
            }
        }
        public void InsertForLatinFileReceivedWithBlobDateTime(string sourceSysId, NsrFileCnvrDataRow currentRecord, string fileName,
                               string fileLoadStatus, DateTime fileCreateDateTime, Int64 fileNumId)
        {

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @" INSERT INTO t_azure_audit_nsr_file_cnvr (src_sys_id, cnfg_src_sys_infa_id, file_seq_id, file_set_id, file_set_sub_id, 
                                                    max_file_set_sub_id, src_file_nm, file_load_status, catalog_dttm, src_file_create_dttm, file_num_id) 
                                                VALUES (@sourceSysId, @infaParamSetId, @fileSeqId, @fileSetId, @fileSetSubId,
                                                    @maxFileSetSubId, @fileName, @fileLoadStatus, @recordTime, @blobCreatedDateTime, @fileNumId)";

                //file_id – auto increment for each new entry
                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var infaParamSetIdParam = cmd.Command.CreateParameter();
                infaParamSetIdParam.DbType = System.Data.DbType.String;
                infaParamSetIdParam.Value = currentRecord.CnfgSrcSysInfaId;
                infaParamSetIdParam.ParameterName = @"infaParamSetId";
                cmd.Command.Parameters.Add(infaParamSetIdParam);

                var fileSeqIdParam = cmd.Command.CreateParameter();
                fileSeqIdParam.DbType = System.Data.DbType.String;
                fileSeqIdParam.Value = currentRecord.FileSeqId;
                fileSeqIdParam.ParameterName = @"fileSeqId";
                cmd.Command.Parameters.Add(fileSeqIdParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = currentRecord.FileSetId;
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                var fileSetSubIdParam = cmd.Command.CreateParameter();
                fileSetSubIdParam.DbType = System.Data.DbType.String;
                fileSetSubIdParam.Value = currentRecord.FileSetSubId;
                fileSetSubIdParam.ParameterName = @"fileSetSubId";
                cmd.Command.Parameters.Add(fileSetSubIdParam);

                var maxFileSetSubIdParam = cmd.Command.CreateParameter();
                maxFileSetSubIdParam.DbType = System.Data.DbType.String;
                maxFileSetSubIdParam.Value = currentRecord.MaxFileSetSubId;
                maxFileSetSubIdParam.ParameterName = @"maxFileSetSubId";
                cmd.Command.Parameters.Add(maxFileSetSubIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var fileLoadStatusParam = cmd.Command.CreateParameter();
                fileLoadStatusParam.DbType = System.Data.DbType.String;
                fileLoadStatusParam.Value = fileLoadStatus;
                fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                cmd.Command.Parameters.Add(fileLoadStatusParam);

                var recordTimeParam = cmd.Command.CreateParameter();
                recordTimeParam.DbType = System.Data.DbType.DateTime;
                recordTimeParam.Value = System.DateTime.UtcNow;
                recordTimeParam.ParameterName = @"recordTime";
                cmd.Command.Parameters.Add(recordTimeParam);

                var blobCreatedDateTimeParam = cmd.Command.CreateParameter();
                blobCreatedDateTimeParam.DbType = System.Data.DbType.DateTime;
                blobCreatedDateTimeParam.Value = fileCreateDateTime;
                blobCreatedDateTimeParam.ParameterName = @"blobCreatedDateTime";
                cmd.Command.Parameters.Add(blobCreatedDateTimeParam);

                var fileNumIdParam = cmd.Command.CreateParameter();
                fileNumIdParam.DbType = System.Data.DbType.Int64;
                fileNumIdParam.Value = fileNumId;
                fileNumIdParam.ParameterName = @"fileNumId";
                cmd.Command.Parameters.Add(fileNumIdParam);

                cmd.Command.ExecuteNonQuery();
            }

        }

        public void InsertForNSRFileReceivedWithCreateDateTime(string sourceSysId, NsrFileCnvrDataRow currentRecord, string fileName,
                                string fileLoadStatus, DateTime fileCreateDateTime, Int64 fileNumId)
        {

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"
                                             IF NOT EXISTS (SELECT src_sys_id, src_file_nm  FROM t_azure_audit_nsr_file_cnvr 
				                                    WHERE src_sys_id=@sourceSysId AND src_file_nm=@fileName AND 
                                                    Convert(varchar(12),src_file_create_dttm,101)+ ' '+ Convert(varchar(10),DATEADD(s,301,src_file_create_dttm),108) > Convert(varchar(12),@blobCreatedDateTime,101)+ ' '+ Convert(varchar(10),@blobCreatedDateTime,108)  )
                                             BEGIN
                                                INSERT INTO t_azure_audit_nsr_file_cnvr (src_sys_id, cnfg_src_sys_infa_id, file_seq_id, file_set_id, file_set_sub_id, 
                                                    max_file_set_sub_id, src_file_nm, file_load_status, catalog_dttm, src_file_create_dttm, file_num_id) 
                                                VALUES (@sourceSysId, @infaParamSetId, @fileSeqId, @fileSetId, @fileSetSubId,
                                                    @maxFileSetSubId, @fileName, @fileLoadStatus, @recordTime, @blobCreatedDateTime, @fileNumId)
                                             END
                                           ";

                //file_id – auto increment for each new entry
                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var infaParamSetIdParam = cmd.Command.CreateParameter();
                infaParamSetIdParam.DbType = System.Data.DbType.String;
                infaParamSetIdParam.Value = currentRecord.CnfgSrcSysInfaId;
                infaParamSetIdParam.ParameterName = @"infaParamSetId";
                cmd.Command.Parameters.Add(infaParamSetIdParam);

                var fileSeqIdParam = cmd.Command.CreateParameter();
                fileSeqIdParam.DbType = System.Data.DbType.String;
                fileSeqIdParam.Value = currentRecord.FileSeqId;
                fileSeqIdParam.ParameterName = @"fileSeqId";
                cmd.Command.Parameters.Add(fileSeqIdParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = currentRecord.FileSetId;
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                var fileSetSubIdParam = cmd.Command.CreateParameter();
                fileSetSubIdParam.DbType = System.Data.DbType.String;
                fileSetSubIdParam.Value = currentRecord.FileSetSubId;
                fileSetSubIdParam.ParameterName = @"fileSetSubId";
                cmd.Command.Parameters.Add(fileSetSubIdParam);

                var maxFileSetSubIdParam = cmd.Command.CreateParameter();
                maxFileSetSubIdParam.DbType = System.Data.DbType.String;
                maxFileSetSubIdParam.Value = currentRecord.MaxFileSetSubId;
                maxFileSetSubIdParam.ParameterName = @"maxFileSetSubId";
                cmd.Command.Parameters.Add(maxFileSetSubIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var fileLoadStatusParam = cmd.Command.CreateParameter();
                fileLoadStatusParam.DbType = System.Data.DbType.String;
                fileLoadStatusParam.Value = fileLoadStatus;
                fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                cmd.Command.Parameters.Add(fileLoadStatusParam);

                var recordTimeParam = cmd.Command.CreateParameter();
                recordTimeParam.DbType = System.Data.DbType.DateTime;
                recordTimeParam.Value = System.DateTime.UtcNow;
                recordTimeParam.ParameterName = @"recordTime";
                cmd.Command.Parameters.Add(recordTimeParam);

                var blobCreatedDateTimeParam = cmd.Command.CreateParameter();
                blobCreatedDateTimeParam.DbType = System.Data.DbType.DateTime;
                blobCreatedDateTimeParam.Value = fileCreateDateTime;
                blobCreatedDateTimeParam.ParameterName = @"blobCreatedDateTime";
                cmd.Command.Parameters.Add(blobCreatedDateTimeParam);

                var fileNumIdParam = cmd.Command.CreateParameter();
                fileNumIdParam.DbType = System.Data.DbType.Int64;
                fileNumIdParam.Value = fileNumId;
                fileNumIdParam.ParameterName = @"fileNumId";
                cmd.Command.Parameters.Add(fileNumIdParam);

                cmd.Command.ExecuteNonQuery();
            }

        }
        //t_audit_file_sub
        public void BottlerFileReceived(string sourceSysId, string fileName, string fileLoadStatus, int moduleId, int fileSetId, string factType, string fileType)
        {
            var fileId = GetProductAuditTableRowId(sourceSysId, fileName);

            if (fileId <= 0)
            {
                using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
                {
                    cmd.Command.CommandText = @"INSERT INTO t_audit_file_sub (file_set_id,src_sys_id,src_file_nm,catalog_dttm,file_load_status,Module_id, fact_type, file_type) 
                                                VALUES (@fileSetId,@sourceSysId,@fileName,@recordTime,@fileLoadStatus,@moduleId, @factType, @fileType)";

                    //file_id – auto increment for each new entry
                    cmd.Command.CommandTimeout = 0;

                    var fileSetIdParam = cmd.Command.CreateParameter();
                    fileSetIdParam.DbType = System.Data.DbType.Int32;
                    fileSetIdParam.Value = fileSetId;                               //Execute sp “nextval” to fetch the file_set_id (file_set_id is same for each file set)
                    fileSetIdParam.ParameterName = @"fileSetId";
                    cmd.Command.Parameters.Add(fileSetIdParam);

                    var sourceSysIdParam = cmd.Command.CreateParameter();
                    sourceSysIdParam.DbType = System.Data.DbType.String;
                    sourceSysIdParam.Value = sourceSysId;                           //Source system id of the submission entity from t_src_sys table
                    sourceSysIdParam.ParameterName = @"sourceSysId";
                    cmd.Command.Parameters.Add(sourceSysIdParam);

                    var fileNameParam = cmd.Command.CreateParameter();
                    fileNameParam.DbType = System.Data.DbType.String;
                    fileNameParam.Value = fileName;                                 //Source file name with extension
                    fileNameParam.ParameterName = @"fileName";
                    cmd.Command.Parameters.Add(fileNameParam);

                    var recordTimeParam = cmd.Command.CreateParameter();
                    recordTimeParam.DbType = System.Data.DbType.DateTime;
                    recordTimeParam.Value = System.DateTime.UtcNow;                 // Time when record is inserted (in UTC)
                    recordTimeParam.ParameterName = @"recordTime";
                    cmd.Command.Parameters.Add(recordTimeParam);

                    var fileLoadStatusParam = cmd.Command.CreateParameter();
                    fileLoadStatusParam.DbType = System.Data.DbType.String;
                    fileLoadStatusParam.Value = fileLoadStatus;                          //AF-READY
                    fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                    cmd.Command.Parameters.Add(fileLoadStatusParam);

                    var moduleIdParam = cmd.Command.CreateParameter();
                    moduleIdParam.DbType = System.Data.DbType.Int32;
                    moduleIdParam.Value = moduleId;                                        //Module id - 2
                    moduleIdParam.ParameterName = @"moduleId";
                    cmd.Command.Parameters.Add(moduleIdParam);

                    var factTypeParam = cmd.Command.CreateParameter();
                    factTypeParam.DbType = System.Data.DbType.String;
                    factTypeParam.Value = factType;                          //AF-READY
                    factTypeParam.ParameterName = @"factType";
                    cmd.Command.Parameters.Add(factTypeParam);

                    var fileTypeParam = cmd.Command.CreateParameter();
                    fileTypeParam.DbType = System.Data.DbType.String;
                    fileTypeParam.Value = fileType;                          //AF-READY
                    fileTypeParam.ParameterName = @"fileType";
                    cmd.Command.Parameters.Add(fileTypeParam);

                    cmd.Command.ExecuteNonQuery();
                }
            }
        }

        //t_audit_file_sub
        public void UpdateStatusForBottlerFileReceived(string sourceSysId, string fileName, string fileLoadStatus, string fileSetId)
        {

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                if (Helpers.IsCurrencyNeutralFile(fileName))
                {
                    cmd.Command.CommandText = @"UPDATE t_audit_file_sub SET file_load_status = @fileLoadStatus 
                                                WHERE src_file_nm = @fileName ";

                }
                else
                {
                    cmd.Command.CommandText = @"UPDATE t_audit_file_sub SET file_load_status = @fileLoadStatus, catalog_dttm = getdate() 
                                            WHERE src_sys_id = @sourceSysId and src_file_nm = @fileName and file_set_id = @fileSetId ";
                }

                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;                           //Source system id of the submission entity from t_src_sys table
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;                                 //Source file name with extension
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var fileLoadStatusParam = cmd.Command.CreateParameter();
                fileLoadStatusParam.DbType = System.Data.DbType.String;
                fileLoadStatusParam.Value = fileLoadStatus;                          //AF-READY
                fileLoadStatusParam.ParameterName = @"fileLoadStatus";
                cmd.Command.Parameters.Add(fileLoadStatusParam);

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = fileSetId;                          //AF-READY
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                cmd.Command.ExecuteNonQuery();
            }
        }

        public void InsertIntoErrorTracker(string sourceSysId, string fileName, string severity, string errorType)
        {
            var idColumns = GetFileId(sourceSysId, fileName);
            if (!idColumns.HasValue) throw new ArgumentException($@"Unable to get file id");

            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"INSERT INTO t_error_tracker (file_set_id, file_id, error_type, severity) 
                                                VALUES (@fileSetId, @fileId, @errorType, @severity)";

                cmd.Command.CommandTimeout = 0;

                var fileSetIdParam = cmd.Command.CreateParameter();
                fileSetIdParam.DbType = System.Data.DbType.String;
                fileSetIdParam.Value = idColumns.Value.FileSetId;                                         //Fetch from t_audit_file_sub
                fileSetIdParam.ParameterName = @"fileSetId";
                cmd.Command.Parameters.Add(fileSetIdParam);

                var fileIdParam = cmd.Command.CreateParameter();
                fileIdParam.DbType = System.Data.DbType.String;
                fileIdParam.Value = idColumns.Value.FileId;                                         //Fetch from t_audit_file_sub
                fileIdParam.ParameterName = @"fileId";
                cmd.Command.Parameters.Add(fileIdParam);

                var errorCdParam = cmd.Command.CreateParameter();
                errorCdParam.DbType = System.Data.DbType.String;
                errorCdParam.Value = errorType;                                       //Fetch from t_error_master
                errorCdParam.ParameterName = @"errorType";
                cmd.Command.Parameters.Add(errorCdParam);

                var severityParam = cmd.Command.CreateParameter();
                severityParam.DbType = System.Data.DbType.String;
                severityParam.Value = severity;                               //ERROR / WARNING               
                severityParam.ParameterName = @"severity";
                cmd.Command.Parameters.Add(severityParam);

                cmd.Command.ExecuteNonQuery();
            }
        }

        //t_error_master
        public string GetErrorId()
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT top 1 error_cd FROM dbo.t_error_master WHERE LTRIM(RTRIM(error_type)) = (@errorType) and LTRIM(RTRIM(severity)) = (@severity)"; // AND src_grp_id = 2";

                cmd.Command.CommandTimeout = 0;

                var errorTypeParam = cmd.Command.CreateParameter();
                errorTypeParam.DbType = System.Data.DbType.String;
                errorTypeParam.Value = "File Validation";
                errorTypeParam.ParameterName = @"errorType";
                cmd.Command.Parameters.Add(errorTypeParam);

                var severityParam = cmd.Command.CreateParameter();
                severityParam.DbType = System.Data.DbType.String;
                severityParam.Value = "ERROR";
                severityParam.ParameterName = @"severity";
                cmd.Command.Parameters.Add(severityParam);

                var errorId = cmd.Command.ExecuteScalar()?.ToString();
                return errorId;
            }
        }

        //t_audit_file_sub
        public (int FileId, int FileSetId)? GetFileId(string sourceSysId, string fileName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_id, file_set_id FROM dbo.t_audit_file_sub 
                                                WHERE src_sys_id = (@sourceSysId) and src_file_nm = (@fileName) order by file_id desc";

                cmd.Command.CommandTimeout = 0;

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.String;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileNameParam = cmd.Command.CreateParameter();
                fileNameParam.DbType = System.Data.DbType.String;
                fileNameParam.Value = fileName;
                fileNameParam.ParameterName = @"fileName";
                cmd.Command.Parameters.Add(fileNameParam);

                var reader = cmd.Command.ExecuteReader();
                if (reader.HasRows && reader.Read())
                {
                    return (reader.GetInt32(0), reader.GetInt32(1));
                }

                return null;
            }
        }

        public string GetTypeForBottler(string fileMask)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT module_nm FROM dbo.t_dmm_module WHERE module_lng_nm = (@fileMask) ";

                var paramFileMask = cmd.Command.CreateParameter();
                paramFileMask.DbType = System.Data.DbType.String;
                paramFileMask.Value = fileMask;
                paramFileMask.ParameterName = @"fileMask";
                cmd.Command.Parameters.Add(paramFileMask);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    string retVal = string.Empty;

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            // TODO: This logic will always only return the last value... is this intended?
                            retVal = reader.GetString(0).ToLowerInvariant();
                        }
                    }

                    return retVal;
                }
            }
        }

        public string GetTypeForBottler(string sourceSysId, string factType, string fileMask)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_type FROM dbo.t_cnfg_src_sys WHERE src_sys_id = (@sourceSysId) and fact_type = (@factType) and file_mask = (@fileMask) ";

                var paramSrcSysId = cmd.Command.CreateParameter();
                paramSrcSysId.DbType = System.Data.DbType.Int32;
                paramSrcSysId.Value = sourceSysId;
                paramSrcSysId.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(paramSrcSysId);

                var paramFactType = cmd.Command.CreateParameter();
                paramFactType.DbType = System.Data.DbType.String;
                paramFactType.Value = factType ?? @"NA";
                paramFactType.ParameterName = @"factType";
                cmd.Command.Parameters.Add(paramFactType);

                var paramFileMask = cmd.Command.CreateParameter();
                paramFileMask.DbType = System.Data.DbType.String;
                paramFileMask.Value = fileMask;
                paramFileMask.ParameterName = @"fileMask";
                cmd.Command.Parameters.Add(paramFileMask);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    string retVal = string.Empty;

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            // TODO: This logic will always only return the last value... is this intended?
                            retVal = reader.GetString(0);
                        }
                    }

                    return retVal;
                }
            }
        }

        public IEnumerable<string> GetExpectedFilesForBottler(string sourceSysId, string factType)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_mask FROM dbo.t_cnfg_src_sys WHERE src_sys_id = (@sourceSysId) and fact_type = (@factType) ";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.Int32;
                param.Value = sourceSysId;
                param.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(param);

                var paramFactType = cmd.Command.CreateParameter();
                paramFactType.DbType = System.Data.DbType.String;
                paramFactType.Value = factType;
                paramFactType.ParameterName = @"factType";
                cmd.Command.Parameters.Add(paramFactType);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            yield return reader.GetString(0).ToLowerInvariant();
                        }
                    }
                    else
                    {
                        throw new KeyNotFoundException($@"The source sys id {sourceSysId} didn't have any file mappings in the config table.");
                    }
                }
            }
        }

        public string GetSourceSysIdForBottlerName(string bottlerName, string containerName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                //cmd.Command.CommandText = @"SELECT src_sys_id FROM dbo.t_src_sys WHERE src_sys_desc = (@bottlerName)"; // AND src_grp_id = 2";
                cmd.Command.CommandText = @"SELECT src_sys_id FROM dbo.t_src_sys WHERE src_btlr_name = (@bottlerName) and azr_blob_cntr = (@containerName)"; // AND src_grp_id = 2";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = bottlerName;
                param.ParameterName = @"bottlerName";
                cmd.Command.Parameters.Add(param);

                var paramCntr = cmd.Command.CreateParameter();
                paramCntr.DbType = System.Data.DbType.String;
                paramCntr.Value = containerName;
                paramCntr.ParameterName = @"containerName";
                cmd.Command.Parameters.Add(paramCntr);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new KeyNotFoundException($@"No Source Sys Id was found in [{nameof(containerName)}: {containerName}] for [{nameof(bottlerName)}: {bottlerName}]");
            }
        }

        public string GetBottlerFolderName(string bottlerName, string containerName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                //cmd.Command.CommandText = @"SELECT src_sys_id FROM dbo.t_src_sys WHERE src_sys_desc = (@bottlerName)"; // AND src_grp_id = 2";
                cmd.Command.CommandText = @"SELECT src_btlr_name FROM dbo.t_src_sys WHERE src_btlr_name = (@bottlerName) and azr_blob_cntr = (@containerName)"; // AND src_grp_id = 2";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = bottlerName;
                param.ParameterName = @"bottlerName";
                cmd.Command.Parameters.Add(param);

                var paramCntr = cmd.Command.CreateParameter();
                paramCntr.DbType = System.Data.DbType.String;
                paramCntr.Value = containerName;
                paramCntr.ParameterName = @"containerName";
                cmd.Command.Parameters.Add(paramCntr);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new KeyNotFoundException($@"No value found for src_btlr_name in [t_src_sys] table for {containerName}/{bottlerName}");
            }
        }

        public IEnumerable<string> GetSourceSysIdListForFileType(string bottlerName)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT src_sys_id FROM dbo.t_src_sys WHERE src_sys_desc = (@bottlerName)"; // AND src_grp_id = 2";

                var param = cmd.Command.CreateParameter();
                param.DbType = System.Data.DbType.String;
                param.Value = bottlerName;
                param.ParameterName = @"bottlerName";
                cmd.Command.Parameters.Add(param);

                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            yield return reader[0].ToString();
                        }
                    }
                }
            }
        }

        public bool HeadersRequiredForBottlerFile(string sourceSysId, string fileType, string factType)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT header_prst FROM dbo.t_cnfg_src_sys WHERE src_sys_id = (@sourceSysId) AND file_type = (@fileType) AND fact_type = (@factType)";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var fileTypeParam = cmd.Command.CreateParameter();
                fileTypeParam.DbType = System.Data.DbType.String;
                fileTypeParam.Value = fileType;
                fileTypeParam.ParameterName = @"fileType";
                cmd.Command.Parameters.Add(fileTypeParam);

                var factTypeParam = cmd.Command.CreateParameter();
                factTypeParam.DbType = System.Data.DbType.String;
                factTypeParam.Value = factType;
                factTypeParam.ParameterName = @"factType";
                cmd.Command.Parameters.Add(factTypeParam);

                return cmd.Command.ExecuteScalar()?.ToString().Equals("Y", StringComparison.OrdinalIgnoreCase) == true;
            }
        }

        public IEnumerable<FieldSpec> GetFieldSpecsForFile(string fileType, string sourceSysId, string factType)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
               // cmd.Command.CommandText = @"SELECT col_nm, data_type, max_data_length, is_mandatory FROM dbo.t_cnfg_file WHERE file_type = (@fileType) ORDER BY col_seq_no ASC";

                cmd.Command.CommandText = @"SELECT col_nm, data_type, max_data_length, is_mandatory 
                                            FROM dbo.t_cnfg_file f
                                                INNER JOIN (SELECT DISTINCT src_sys_id, file_type, fact_type, file_vrsn FROM t_cnfg_src_sys) s 
                                            	    ON f.file_type = s.file_type AND f.file_vrsn = s.file_vrsn
                                            WHERE f.file_type = (@fileType) AND s.src_sys_id = (@sourceSysId)
                                                AND (CASE WHEN f.file_type = 'DSC-MUL' THEN 1 WHEN s.fact_type = (@factType) THEN 1 ELSE 0 END) = 1";

                var fileTypeParam = cmd.Command.CreateParameter();
                fileTypeParam.DbType = System.Data.DbType.String;
                fileTypeParam.Value = fileType.ToLowerInvariant();
                fileTypeParam.ParameterName = @"fileType";
                cmd.Command.Parameters.Add(fileTypeParam);

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                var factTypeParam = cmd.Command.CreateParameter();
                factTypeParam.DbType = System.Data.DbType.String;
                factTypeParam.Value = factType.ToLowerInvariant();
                factTypeParam.ParameterName = @"factType";
                cmd.Command.Parameters.Add(factTypeParam);


                using (var reader = cmd.Command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            yield return new FieldSpec
                            {
                                Description = reader.GetString(0),
                                DataType = reader.GetString(1),
                                MaxDataLength = (reader[2] == DBNull.Value) ? null : (int?)reader.GetInt32(2),
                                MustNotBeNull = (reader[3] == DBNull.Value) ? null : (bool?)reader.GetBoolean(3)
                            };
                        }
                    }
                }
            }
        }

        public string GetFileTypeForUiFile(string sourceSysId)
        {
            using (var cmd = new NsrSqlCommand(_configurationDatabaseConnection.OpenAndCreateCommand()))
            {
                cmd.Command.CommandText = @"SELECT file_type FROM dbo.t_cnfg_src_sys WHERE src_sys_id = (@sourceSysId) ";

                var sourceSysIdParam = cmd.Command.CreateParameter();
                sourceSysIdParam.DbType = System.Data.DbType.Int32;
                sourceSysIdParam.Value = sourceSysId;
                sourceSysIdParam.ParameterName = @"sourceSysId";
                cmd.Command.Parameters.Add(sourceSysIdParam);

                return cmd.Command.ExecuteScalar()?.ToString() ?? throw new ArgumentException($@"No single filetype found for srcSysId {sourceSysId}", nameof(sourceSysId));
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _configurationDatabaseConnection.Close();
                    _configurationDatabaseConnection.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion

        class NsrSqlCommand : IDisposable
        {
            public NsrSqlCommand(SqlCommand sqlCommand)
            {
                this.Command = sqlCommand;
            }

            public SqlCommand Command { get; }

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        this.Command.Connection.Close();
                        this.Command.Dispose();
                    }

                    disposedValue = true;
                }
            }

            public void Dispose()
            {
                // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
                Dispose(true);
            }
            #endregion
        }
    }

    struct FieldSpec
    {
        public string Description { get; set; }
        public string DataType { get; set; }
        public int? MaxDataLength { get; set; }
        public bool? MustNotBeNull { get; set; }
    }

}


