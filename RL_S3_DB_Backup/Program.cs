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

            DirectoryInfo parentDirectory;
           
            string rootFolderPath = args[0];

            parentDirectory = new DirectoryInfo(rootFolderPath);

            //this will be the name of the folder in S3
            string parentFolderNameAgg = parentDirectory.Name;

            //for testing
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
                try
                {
                    if (!BeenModified(file, key))
                    {
                        Console.WriteLine("Up to date.");
                        Console.WriteLine();
                        continue;
                    }
                    //if the file has been modified, zip and upload it (after catch block)...
                    Console.WriteLine("Done");
                }
                //if checking the last write time didn't work for some reason, log the error...
                catch (Exception ex)
                {
                    string message = "An error occured while verifying the last write time of file " +
                        file.Name + ". The metadata in S3 may be corrupted. See error log for more details." + "\n" +
                        //S3 always returns an error to a metadata request for a file that has not yet been uploaded:
                        "If this is the first time this file is being uploaded, ignore this error.";
                    PrintCustomErrorMessage(message);
                    LogException(ex, file.Name);
                }//...and zip and upload the file to ensure that there will be an up-to-date backup of this file:

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

            //delete temp archive
            File.Delete(archiveName);

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

        private static void PrintCustomErrorMessage(string message)
        {
            Console.WriteLine();
            Console.WriteLine(message);
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
