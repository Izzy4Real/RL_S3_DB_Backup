using System.Configuration;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.IO;
using System.IO.Compression;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;

namespace UploadDirectoryWithMetadata
{
    class Program
    {
        static string _bucketName = ConfigurationManager.AppSettings["bucketName"];

        static void Main(string[] args)
        {
            Console.WriteLine();
            System.Net.ServicePointManager.SecurityProtocol = (System.Net.SecurityProtocolType)(3072);

            //var appSettings = ConfigurationManager.AppSettings;

            ////if app.config appsettings section AWSProfilesLocation is empty...
            //if (String.IsNullOrEmpty(appSettings["AWSProfilesLocation"]))
            //{
            //    //do initial setup
            //    Setup();
            //}

            ////if user wants to change settings
            //if (Prompt("Enter '1' to start setup. Press enter to skip setup") == "1")
            //{
            //    Setup();
            //}

            DirectoryInfo parentDirectory;
            //while (true)
            //{
            //    string rootFolderPath = //@"C:\Users\Avshalom Gotlib\Documents\Job Search\Remote Landlord\AWS\RLDocuments_Use_S3\TenantDocs";
            //                            Prompt("Enter absolute path of directory to upload:");
            //    Console.WriteLine();

            //    if (Directory.Exists(rootFolderPath))
            //    {
            //        parentDirectory = new DirectoryInfo(rootFolderPath);
            //        break;
            //    }

            //    Console.WriteLine("Folder not found. Please check the path of the folder.");
            //    Console.WriteLine();
            //}
            string rootFolderPath = args[0];

            //if (!Directory.Exists(rootFolderPath))
            //{
            //    Console.WriteLine("Folder not found. Please check the path of the folder.");
            //    Console.WriteLine();
            //    continue;
            //}
            parentDirectory = new DirectoryInfo(rootFolderPath);

            //Console.WriteLine("Uploading...");
            //Console.WriteLine();


            //this will be the name of the folder in S3
            string parentFolderNameAgg = parentDirectory.Name;

            //Stopwatch sw = Stopwatch.StartNew();

            UploadFilesInFolderWithMetadata(parentDirectory, parentFolderNameAgg);

            //for testing
            //Console.WriteLine(sw.Elapsed);
            //Console.ReadKey(true);
        }

        static void UploadFilesInFolderWithMetadata(DirectoryInfo parentFolder, string parentFolderNameAgg)
        {
            IEnumerable<FileInfo> files = parentFolder.GetFiles().Where(fi => fi.Extension != ".zip");

            string archiveName = parentFolder.FullName + "\\TempArchive.zip";

            foreach (FileInfo file in files)
            {
                //the S3 Key for this file
                string key = String.Empty;

                //for testing:
                //key += "BackupTest/";

                key += parentFolderNameAgg + "/";
                key += Path.GetFileNameWithoutExtension(file.Name) + ".zip";

                Console.Write("Verifying status of {0}...", file.Name);

                //Check to see if the file has been modified since the last time it was uploaded to S3.
                //If it has not been modified, move to the next file.
                if (!BeenModified(file, key))
                {
                    Console.WriteLine("Up to date.");
                    Console.WriteLine();
                    continue;
                }
                Console.WriteLine("Done");

                Console.Write("Zipping {0}...", file.Name);
                using (Stream archiveStream = File.Create(archiveName))
                using (ZipArchive archive = new ZipArchive(archiveStream, ZipArchiveMode.Update))
                {
                    //zip the file to the archive
                    ZipArchiveEntry entry = archive.CreateEntryFromFile(file.FullName, file.Name);
                }
                Console.WriteLine("Done.");

                //prepare the request, including the original creation time of the file
                var pRequest = new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = key,
                    FilePath = archiveName
                    //StorageClass = S3StorageClass.StandardInfrequentAccess
                };

                //do we want creation time or Last write time?
                //pRequest.Metadata.Add("creation-time", file.CreationTime.ToString());
                pRequest.Metadata.Add("last-write-time", file.LastWriteTime.ToString());

                Console.Write("Uploading {0}...", file.Name);
                try
                {
                    //upload to S3
                    using (var client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
                    {
                        //_client.PutObject(pRequest);
                        client.PutObject(pRequest);
                    }
                    Console.WriteLine("Done.");
                }
                catch (Exception ex)
                {
                    PrintErrorMessageUpload(ex, key);
                    LogException(ex, key);
                }
                Console.WriteLine();
            }

            Console.Write("Clearing archive...");
            //clean up the archive
            using (Stream archiveStream = File.Open(archiveName, FileMode.OpenOrCreate))
            using (ZipArchive archive = new ZipArchive(archiveStream, ZipArchiveMode.Update))
            {
                ZipArchiveEntry entry = archive.Entries.FirstOrDefault();
                if (entry != null)
                {
                    entry.Delete();
                }
            }
            Console.WriteLine("Done.");

            //recurse not necessary on production machine
            //foreach (DirectoryInfo directory in parentFolder.GetDirectories())
            //{
            //    //the key of this folder in S3
            //    string key = parentFolderNameAgg + "/" + directory.Name + "/";

            //    Console.WriteLine(directory.Name);

            //    //prepare the request, including the original creation time of the folder
            //    var pRequest = new PutObjectRequest
            //    {
            //        //BucketName = bucketName,
            //        BucketName = _bucketName,
            //        Key = key
            //    };
            //    //do we want creation time or Last write time?
            //    //pRequest.Metadata.Add("creation-time", directory.CreationTime.ToString());
            //    pRequest.Metadata.Add("last-write-time", directory.LastWriteTime.ToString());

            //    try
            //    {
            //        //upload the folder
            //        using (var client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
            //        {
            //            client.PutObject(pRequest);
            //        }
            //    }
            //    catch (Exception ex)
            //    {
            //        PrintErrorMessageUpload(ex, key);
            //        LogException(ex, key);
            //    }

            //    //recurse through the folder
            //    UploadFilesInFolderWithMetadata(directory, parentFolderNameAgg + "/" + directory.Name);
            //}
        }

        private static bool BeenModified(FileInfo file, string key)
        {
            try
            {
                //get the last write time from the metadata (will be a string)
                GetObjectMetadataResponse lResponse;
                using (var client = new AmazonS3Client(Amazon.RegionEndpoint.USEast1))
                {
                    //lResponse = _client.GetObjectMetadata(_bucketName, key);
                    lResponse = client.GetObjectMetadata(_bucketName, key);
                }

                //parse to a datetime
                DateTime s3LastWriteTime = DateTime.Parse(lResponse.Metadata["last-write-time"]);

                //convert the file's last write time into a date time accurate to the nearest second.
                string simpleLastWriteTimeString = file.LastWriteTime.ToString();
                DateTime simpleLastWriteTime = DateTime.Parse(simpleLastWriteTimeString);

                //return true if file last write time is later than the last write time in the S3 backup.
                return simpleLastWriteTime > s3LastWriteTime;

            }
            catch (Exception ex)
            {
                //if something went wrong, print and log the exception, then return true as if the file has been modified.
                //that way, the file will be backed up now, and will recieve last write time metadata from this point.
                string message = "An error occured while verifying the last write times of file: " +
                                 file.FullName + ".\n" + "The Metadata in S3 may be corrupted.\n" +
                                 "If this is the first time this file is being uploaded, ignore this error.";
                PrintCustomErrorMessage(ex, message);
                LogException(ex, file.FullName);

                return true;
            }
        }

        //private static void Setup()
        //{
        //    Console.WriteLine();
        //    Console.WriteLine("-----Setup-----");

        //    var configFile = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
        //    var settings = configFile.AppSettings.Settings;

        //    while (true)
        //    {
        //        string credentialsFileLocation = Prompt("Enter absolute path of credentials file (press enter to skip this step):");
        //        if (String.IsNullOrEmpty(credentialsFileLocation))
        //        {
        //            break;
        //        }
        //        if (File.Exists(credentialsFileLocation))
        //        {
        //            settings["AWSProfilesLocation"].Value = credentialsFileLocation;
        //            Console.WriteLine();
        //            break;
        //        }

        //        Console.WriteLine("File not found. Please check the file path.");
        //        Console.WriteLine();
        //    }

        //    string bucketName = Prompt("Enter destination bucket name (press enter to skip this step):");
        //    if (!String.IsNullOrEmpty(bucketName))
        //    {
        //        settings["bucketName"].Value = bucketName;
        //    }

        //    configFile.Save();
        //    ConfigurationManager.RefreshSection("appSettings");

        //    Console.WriteLine("-----Setup Complete-----");
        //    Console.WriteLine();
        //}

        static string Prompt(string text)
        {
            Console.WriteLine(text);
            return Console.ReadLine();
        }

        private static void PrintErrorMessageUpload(Exception ex, string fileName)
        {
            Console.WriteLine();
            Console.WriteLine("An error occured while uploading: " + fileName);
            Console.WriteLine("See Error Log for more details");
            Console.WriteLine(ex);
            Console.WriteLine();
        }

        private static void PrintCustomErrorMessage(Exception ex, string message)
        {
            Console.WriteLine();

            Console.WriteLine(message);
            Console.WriteLine();

            Console.WriteLine(ex);
            Console.WriteLine();
        }

        public static void LogException(Exception ex, string fileName)
        {
            string errorLog = Path.GetFullPath("ErrorLog.txt");
            StreamWriter writer = new StreamWriter(errorLog, true);
            writer.WriteLine("********** {0} **********", DateTime.Now);
            writer.WriteLine("An error occured while uploading " + fileName);
            if (ex.InnerException != null)
            {
                writer.Write("Inner Exception Type: ");
                writer.WriteLine(ex.InnerException.GetType().ToString());
                writer.Write("Inner Exception: ");
                writer.WriteLine(ex.InnerException.Message);
                writer.Write("Inner Source: ");
                writer.WriteLine(ex.InnerException.Source);
                if (ex.InnerException.StackTrace != null)
                {
                    writer.Write("Stack Trace: ");
                    writer.WriteLine(ex.InnerException.StackTrace);
                }
            }
            writer.Write("Exception Type: ");
            writer.WriteLine(ex.GetType().ToString());
            writer.WriteLine("Exception: " + ex.Message);
            writer.WriteLine("Stack Trace: ");
            if (ex.StackTrace != null)
            {
                writer.WriteLine(ex.StackTrace);
                writer.WriteLine();
            }
            writer.Close();
        }
    }
}
