using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace test_sit_cli
{
	using FileQueueItem = Tuple<string>;
	using DbQueueItem = Tuple<string, byte[], string>;

	static class Program
	{
		private static ManagedQueue<FileQueueItem> fileQueue;
		private static ManagedQueue<DbQueueItem> hashQueue;

		static void Main(string[] args)
		{
			fileQueue = new ManagedQueue<FileQueueItem>();
			hashQueue = new ManagedQueue<DbQueueItem>();

			DB db = new DB(args[0], args[1], ref hashQueue);
			db.Connect();

			PathScanner pathScanner = new PathScanner(ref fileQueue);
			pathScanner.StartAsyncScan(args.Skip(3).ToArray());

			FileHashComputer hashComputer = new FileHashComputer(ref fileQueue, ref hashQueue);
			hashComputer.StartAsyncHashComputer(Convert.ToUInt32(args[2]));

			db.StartAsyncThread();

			Console.ReadLine();
		}

	}

	public class ManagedQueue<T> : Queue<T>
	{
		public Object SyncRoot { get; }

		public bool IsCompleted { get; private set; }

		public ManagedQueue()
		{
			SyncRoot = new Object();
			IsCompleted = false;
		}

		public void Complete()
		{
			IsCompleted = true;
		}

		public void Wait()
		{
			Monitor.Wait(SyncRoot);
		}

		public void Pulse()
		{
			Monitor.Pulse(SyncRoot);
		}

		public void PulseAll()
		{
			Monitor.PulseAll(SyncRoot);
		}
	}

	public class PathScanner
	{
		private ManagedQueue<FileQueueItem> fileQueue;
		private uint filesFound;

		public PathScanner(ref ManagedQueue<FileQueueItem> fileQueue)
		{
			this.fileQueue = fileQueue;
		}

		public void StartAsyncScan(string[] paths)
		{
			new Thread(new ParameterizedThreadStart(AsyncScanThread)) { IsBackground = true }.Start(paths);
		}

		private void AsyncScanThread(object paths)
		{
			Console.WriteLine("PathScanner has started");
			foreach (var path in (string[])paths)
			{
				try
				{
					var files = Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories);
					foreach (var file in files)
					{
						lock (fileQueue.SyncRoot)
						{
							fileQueue.Enqueue(new FileQueueItem(file));
							fileQueue.Pulse();
						}
						filesFound++;
					}
				}
				catch (UnauthorizedAccessException)
				{

				}
				catch (PathTooLongException)
				{

				}
				catch (DirectoryNotFoundException)
				{

				}
			}

			lock (fileQueue.SyncRoot)
			{
				fileQueue.Complete();
				fileQueue.PulseAll();
			}
			Console.WriteLine("PathScanner has finished ({0} files found)", filesFound);
		}
	}

	public class FileHashComputer
	{
		private ManagedQueue<FileQueueItem> fileQueue;
		private ManagedQueue<DbQueueItem> hashQueue;

		private volatile static int activeComputeHashThreads;

		public FileHashComputer(ref ManagedQueue<FileQueueItem> fileQueue, ref ManagedQueue<DbQueueItem> hashQueue)
		{
			this.fileQueue = fileQueue;
			this.hashQueue = hashQueue;

			activeComputeHashThreads = 0;
		}

		public void StartAsyncHashComputer(uint processes)
		{
			activeComputeHashThreads = 0;
			for (int i = 0; i < processes; i++)
			{
				new Thread(ComputeHashThread) { IsBackground = true }.Start();
			}
		}

		private void ComputeHashThread()
		{
			var threadId = Interlocked.Increment(ref activeComputeHashThreads);
			Console.WriteLine("HashComputer {0} has started", threadId);

			FileQueueItem item;
			byte[] hash;
			string note;
			var md5 = MD5.Create();

			while (true)
			{
				lock (fileQueue.SyncRoot)
				{
					if (fileQueue.Count == 0)
					{
						if (fileQueue.IsCompleted) break;
						fileQueue.Wait();
					}
					item = fileQueue.Dequeue();
				}

				hash = null;
				note = "";

				try
				{
					var stream = File.OpenRead(item.Item1);
					hash = md5.ComputeHash(stream);
					stream.Close();
				}
				catch (Exception e)
				{
					note = e.Message;
				}

				lock (hashQueue.SyncRoot)
				{
					hashQueue.Enqueue(new DbQueueItem(item.Item1, hash, note));
					hashQueue.Pulse();
				}
			}

			var threadsRemaining = Interlocked.Decrement(ref activeComputeHashThreads);
			Console.WriteLine("HashComputer {0} has finished ({1} remaining)", threadId, threadsRemaining);
			if (threadsRemaining == 0)
			{
				lock (hashQueue.SyncRoot)
				{
					hashQueue.Complete();
					hashQueue.PulseAll();
				}
			}
		}
	}

	public class DB
	{
		private SqlConnection conn;
		private SqlCommand command;
		private ManagedQueue<DbQueueItem> hashQueue;

		public DB(string dataSource, string initialCatalog, ref ManagedQueue<DbQueueItem> hashQueue)
		{
			var connectionString = new SqlConnectionStringBuilder();
			connectionString.DataSource = dataSource;
			connectionString.InitialCatalog = initialCatalog;
			connectionString.IntegratedSecurity = true;
			conn = new SqlConnection(connectionString.ToString());
			command = new SqlCommand { Connection = conn };
			this.hashQueue = hashQueue;
		}

		public void Connect()
		{
			conn.Open();
		}

		public void StartAsyncThread()
		{
			new Thread(Iterator) { IsBackground = true }.Start();
		}

		private void Iterator()
		{
			command.CommandText = "DROP TABLE IF EXISTS hashtable";
			command.ExecuteNonQuery();
			command.CommandText = "CREATE TABLE hashtable (id INT IDENTITY(1,1) PRIMARY KEY, path VARCHAR(512), hash BINARY(16), notes VARCHAR(768))";
			command.ExecuteNonQuery();

			command.CommandText = "INSERT INTO hashtable (path,hash,notes) VALUES (@path,@hash,@notes)";
			command.Parameters.AddWithValue("@path", null);
			command.Parameters.AddWithValue("@hash", null);
			command.Parameters.AddWithValue("@notes", null);

			DbQueueItem item;

			while (true)
			{
				lock (hashQueue.SyncRoot)
				{
					if (hashQueue.Count == 0)
					{
						if (hashQueue.IsCompleted) break;
						hashQueue.Wait();
					}
					item = hashQueue.Dequeue();
				}

				command.Parameters[0].Value = item.Item1;
				command.Parameters[1].Value = item.Item2 ?? System.Data.SqlTypes.SqlBinary.Null;
				command.Parameters[2].Value = item.Item3;
				command.ExecuteNonQuery();

				Console.WriteLine(item.ToString());
			}

			conn.Close();

			Console.WriteLine("DB Iterator has finished");
		}
	}
}
