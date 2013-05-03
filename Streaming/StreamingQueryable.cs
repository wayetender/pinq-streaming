using System;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Collections.ObjectModel;
using System.Threading;
using PINQ;

namespace PINQ.Streaming
{

	public abstract class PINQStreamingAgent : PINQAgent
	{
		private double budget;
		private double originalBudget;
		public PINQStreamingAgent(double budget)
		{
			this.budget = budget;
			this.originalBudget = budget;
		}
		public abstract bool ApplyEventLevel(double epsilon);
		public abstract void UnapplyEventLevel(double epsilon);
		public abstract bool ApplyUserLevel(double epsilon);
		public abstract void UnapplyUserLevel(double epsilon);
		public override bool apply (double epsilon)
		{
			if (epsilon > budget)
				return false;
			
			budget -= epsilon;
			return true;
		}

		public virtual void unapply (double epsilon)
		{
			budget += epsilon;

			if (originalBudget < budget)
				throw new Exception("current budget greater than original budget");
		}
	}

	public class PINQEventLevelAgent : PINQStreamingAgent
	{
		public PINQEventLevelAgent(double budget) : base(budget)
		{
		}

		public override bool ApplyEventLevel(double epsilon)
		{
			return apply(epsilon);
		}
		public override bool ApplyUserLevel(double epsilon)
		{
			return apply(epsilon);
		}
		public override void UnapplyEventLevel(double epsilon)
		{
			unapply(epsilon);
		}
		public override void UnapplyUserLevel(double epsilon)
		{
			unapply(epsilon);
		}
	}

	public class PINQUserLevelAgent : PINQStreamingAgent
	{
		public PINQUserLevelAgent(double budget) : base(budget)
		{
		}
		
		public override bool ApplyEventLevel(double epsilon)
		{
			return apply(epsilon);
		}
		public override bool ApplyUserLevel(double epsilon)
		{
			return apply(epsilon);
		}
		public override void UnapplyEventLevel(double epsilon)
		{
			// Do nothing, the budget is gone
		}
		public override void UnapplyUserLevel(double epsilon)
		{
			// Do nothing, the budget is gone
		}
	}

	public abstract class StreamingDataSource<T>
	{
		public Action<T> EventReceived { get; set; }
		public FilteredStreamingDataSource<T> filter(Func<T, bool> predicate)
		{
			return new FilteredStreamingDataSource<T>(this, predicate);
		}
		public MappedStreamingDataSource<T,TP> map<TP>(Func<T, TP> mapper)
		{
			return new MappedStreamingDataSource<T, TP>(this, mapper);
		}
		public virtual void Stop() { }
	}

	
	public class FilteredStreamingDataSource<T> : StreamingDataSource<T>
	{
		private Func<T, bool> predicate;
		public FilteredStreamingDataSource(StreamingDataSource<T> baseStream, Func<T, bool> predicate)
		{
			this.predicate = predicate;
			baseStream.EventReceived += filter;
		}
		private void filter(T data)
		{
			if (data != null && predicate(data))
			{
				EventReceived(data);
			}
			else
			{
				EventReceived(default (T));
			}
		}
	}
	
	public class MappedStreamingDataSource<S,T> : StreamingDataSource<T>
	{
		private Func<S, T> transform;
		public MappedStreamingDataSource(StreamingDataSource<S> baseStream, Func<S, T> transform)
		{
			this.transform = transform;
			baseStream.EventReceived += mapper;
		}
		private void mapper(S data)
		{
			if (data != null)
			{
				EventReceived(transform(data));
			}
			else
			{
				EventReceived(default (T));
			}
		}
	}

	public class FunctionStream<T> : StreamingDataSource<T>
	{
		private Func<int, T> func;
		private int sleepMs;
		private Thread thread;
		private bool running;
		private int i;
		public FunctionStream(int sleepMs, Func<int, T> func)
		{
			this.sleepMs = sleepMs;
			this.func = func;
			thread = new Thread(Run);
			i = 0;
			running = true;
			thread.Start();
		}

		public void Run()
		{
			while (running)
			{
				if (EventReceived != null)
				{
					EventReceived(func(i++));
				}
				try
				{
					Thread.Sleep(sleepMs);
				} 
				catch (ThreadInterruptedException)
				{
				}
			}
		}
		
		public override void Stop ()
		{
			if (thread.IsAlive)
			{
				running = false;
				thread.Interrupt();
				thread.Join();
			}
		}
	}

	public class RandomNumbers : StreamingDataSource<Double>
	{
		protected static Random random = new Random();

		private double low;
		private double high;
		private int sleepMs;
		private Thread thread;
		private bool running;
		public RandomNumbers(double low, double high, int sleepMs)
		{
			this.low = low;
			this.high = high;
			this.sleepMs = sleepMs;
			thread = new Thread(Run);
			running = true;
			thread.Start();
		}

		public void Run()
		{
			while (running)
			{
				if (EventReceived != null)
				{
					EventReceived(Uniform());
				}
				try
				{
					Thread.Sleep(sleepMs);
				} 
				catch (ThreadInterruptedException)
				{
				}
			}
		}

		public override void Stop ()
		{
			if (thread.IsAlive)
			{
				running = false;
				thread.Interrupt();
				thread.Join();
			}
		}

		private double Uniform()
		{
			return low + (high - low) * random.NextDouble();
		}


	}


	public class NumbersFromConsole : StreamingDataSource<int>
	{
		private Thread thread;
		private Boolean running;
		public NumbersFromConsole()
		{
			thread = new Thread(Run);
			running = true;
			thread.Start();
		}
		public override void Stop()
		{
			if (thread.IsAlive)
			{
				running = false;
				thread.Interrupt();
				thread.Join();
			}
		}

		private void Run()
		{
			while (running)
			{
				try
				{
					String s = Console.ReadLine();
					if (EventReceived != null)
					{
						if (!string.IsNullOrEmpty(s))
						{
							EventReceived(int.Parse(s));
						}
					}
				}
				catch (ThreadInterruptedException)
				{
				}
			}
		}

	}


	public class StreamingQueryable<T>
	{
		private StreamingDataSource<T> data;
		private PINQStreamingAgent agent;
		private List<StreamingAlgorithm<T>> activeAlgorithms;
		private List<StreamingAlgorithm<T>> toRemove;
		private bool processing;

		public StreamingQueryable(StreamingDataSource<T> data, PINQStreamingAgent agent)
		{
			this.data = data;
			this.agent = agent;
			this.activeAlgorithms = new List<StreamingAlgorithm<T>>();
			data.EventReceived += eventReceived;
			toRemove = new List<StreamingAlgorithm<T>>();
		}

		public StreamingQueryable<T> Where(Expression<Func<T, bool>> predicate)
		{
			return new StreamingQueryable<T>(data.filter(predicate.Compile()), agent); 
		}

		public StreamingQueryable<S> Select<S>(Expression<Func<T, S>> selector)
		{
			return new StreamingQueryable<S>(data.map(selector.Compile()), agent);
		}

		public void RegisterAlgorithm(StreamingAlgorithm<T> alg)
		{
			lock(this)
			{
				if (alg is StreamingUserAlgorithm<T>)
				{
					if (!agent.ApplyUserLevel(alg.Epsilon))
					{
						throw new Exception("PINQ access denied");
					}
				} // otherwise, we will apply when we receive the data

				activeAlgorithms.Add(alg);
			}
		}

		public void UnregisterAlgorithm(StreamingAlgorithm<T> alg)
		{
			lock(this)
			{
				if (alg is StreamingUserAlgorithm<T>)
				{
					agent.UnapplyUserLevel(alg.Epsilon);
				} // otherwise, we will unapply after we receive the data

				if (processing)
					toRemove.Add(alg);
				else
					activeAlgorithms.Remove(alg);
			}
		}

		private void eventReceived(T input)
		{
			lock(this)
			{
				processing = true;
				applyEventLevelAlgorithms();

				foreach (StreamingAlgorithm<T> alg in activeAlgorithms)
				{
					alg.EventReceived(input);
				}

				unapplyEventLevelAlgorithms();
				processing = false;
				activeAlgorithms.RemoveAll(alg => toRemove.Contains(alg));
				toRemove.Clear();
			}
		}

		private void applyEventLevelAlgorithms()
		{
			foreach (StreamingAlgorithm<T> alg in activeAlgorithms)
			{
				if (alg is StreamingEventAlgorithm<T>)
				{
					if (!agent.ApplyEventLevel(alg.Epsilon))
					{
						throw new Exception("PINQ access denied");
					}
				}
			}
		}

		private void unapplyEventLevelAlgorithms()
		{
			foreach (StreamingAlgorithm<T> alg in activeAlgorithms)
			{
				if (alg is StreamingEventAlgorithm<T>)
				{
					agent.UnapplyEventLevel(alg.Epsilon);
				}
			}
		}

		#region Algorithms
		
		public StreamingEventAlgorithm<T> NoisyAverage(double epsilon, Expression<Func<T, double>> function)
		{
			return new NoisyAverageStreaming<T>(this, epsilon, function);
		}

		public StreamingEventAlgorithm<T> RandomizedResponseCount(double epsilon)
		{
			return new RandomizedResponseCount<T>(this, epsilon);
		}

		public StreamingEventAlgorithm<T> BinaryCount(double epsilon, int maxT)
		{
			return new BinaryCount<T>(this, epsilon, maxT);
		}

		public UserDensity<T> UserDensity(double epsilon, List<T> universe, double accuracy, double confidence = 0.90) 
		{
			return new UserDensity<T>(this, epsilon, universe, accuracy, confidence);
		}

		public UserDensityContinuous<T> UserDensityContinuous(double epsilon, List<T> universe, double accuracy, int maxT, double confidence = 0.90)
		{
			return new UserDensityContinuous<T>(this, epsilon, universe, accuracy, confidence, maxT);
		}

		#endregion 

	}

	public abstract class StreamingAlgorithm<T>
	{
		protected static System.Random random = new System.Random();

		protected Semaphore eventProcessed;

		public StreamingAlgorithm(StreamingQueryable<T> dataSource, double epsilon)
		{
			this.DataSource = dataSource;
			this.Epsilon = epsilon;
			this.eventProcessed = new Semaphore(0, 1);
		}

		protected void signalEndProcessed()
		{
			try
			{
				eventProcessed.Release();
			}
			catch (Exception)
			{
				//ignore
			}

		}

		public void ProcessEvents(int n)
		{
			if (!IsReceivingData)
			{
				StartReceiving();
			}

			int startCount = EventsSeen;
			while (startCount + n > EventsSeen && IsReceivingData)
			{
				try
				{
					eventProcessed.WaitOne();
				}
				catch(ThreadInterruptedException)
				{
				}
			}
		}

		public Action<double> OnOutput { get; set; }

		public bool IsReceivingData { get; protected set; }

		public StreamingQueryable<T> DataSource { get; private set; }

		public int EventsSeen { get; protected set; }

		public double Epsilon { get; private set; }

		public double? LastOutput { get; protected set; }

		public abstract double GetOutput();

		public virtual void StartReceiving()
		{
			DataSource.RegisterAlgorithm(this);
			IsReceivingData = true;
		}

		public virtual void StopReceiving()
		{
			DataSource.UnregisterAlgorithm(this);
			IsReceivingData = false;
		}

		protected double Laplace(double stddev)
		{
			double uniform = random.NextDouble() - 0.5;
			return stddev * Math.Sign(uniform) * Math.Log(1 - 2.0 * Math.Abs(uniform));
		}

		protected double Uniform(double low, double high)
		{
			return low + (high - low) * random.NextDouble();
		}

		public virtual void EventReceived(T data)
		{
			EventsSeen++;
		}

		protected int IncrementValue(T input)
		{
			int increment = 0;
			if (!default(T).Equals(input))
			{
				increment = 1;
			}
			return increment;
		}
		
	}

	public abstract class StreamingEventAlgorithm<T> : StreamingAlgorithm<T>
	{
		public StreamingEventAlgorithm(StreamingQueryable<T> s, double epsilon) : base(s, epsilon)
		{
		}
		
		public override double GetOutput ()
		{
			return LastOutput.Value;
		}
	}

	
	public abstract class StreamingUserAlgorithm<T> : StreamingAlgorithm<T>
	{
		public StreamingUserAlgorithm(StreamingQueryable<T> s, double epsilon) : base(s, epsilon)
		{
		}
	}

	public abstract class BufferedAlgorithm<T> : StreamingEventAlgorithm<T>
	{
		protected List<T> events;
		public BufferedAlgorithm(StreamingQueryable<T> s, double epsilon) : base(s, epsilon)
		{
			events = new List<T>();
		}
		public override void EventReceived(T data)
		{
			base.EventReceived(data);
			events.Add (data);
			signalEndProcessed();

		}
	}

	public class NoisyAverageStreaming<T> : BufferedAlgorithm<T>
	{
		private Expression<Func<T, double>> function;
		public NoisyAverageStreaming(StreamingQueryable<T> s, double epsilon, Expression<Func<T, double>> function) : base(s, epsilon)
		{
			this.function = function;
		}

		public override double GetOutput ()
		{
			StopReceiving();
			//Console.WriteLine("data is " + string.Join(", ", events.Select(n => n.ToString()).ToArray()));
			if (!LastOutput.HasValue)
				LastOutput = new PINQueryable<T>(events.AsQueryable(), new PINQAgentBudget(Epsilon)).NoisyAverage(Epsilon, function);
		
			return LastOutput.Value;
		}
	}

	public class RandomizedResponseCount<T> : StreamingEventAlgorithm<T>
	{
		public RandomizedResponseCount(StreamingQueryable<T> s, double epsilon) : base(s, epsilon)
		{
			LastOutput = Laplace(1.0 / Epsilon);
		}

		public override void EventReceived (T data)
		{
			base.EventReceived (data);
			double increment = IncrementValue(data);
			LastOutput += increment + Laplace(1.0 / Epsilon);
			
			if (OnOutput != null)
			{
				OnOutput(LastOutput.Value);
			}

			signalEndProcessed();
		}
	}

	public class BinaryCount<T> : StreamingEventAlgorithm<T>
	{
		private int maxSteps = 0;
		private double internalEpsilon;
		private int logT;
		private int[] partialSums;
		private double[] noisyPartialSums;
		public BinaryCount(StreamingQueryable<T> s, double epsilon, int maxSteps) : base(s, epsilon)
		{
			this.maxSteps = maxSteps;
			logT = Convert.ToInt32(Math.Log(maxSteps, 2) + 0.5);
			internalEpsilon = epsilon / (double)logT;
			LastOutput = Laplace(1.0 / Epsilon);
			partialSums = new int[logT];
			noisyPartialSums = new double[logT];
		}

		public override void EventReceived (T data)
		{
			base.EventReceived (data);
			if (EventsSeen > maxSteps)
			{
				StopReceiving();
				return;
			}

			int val = IncrementValue(data);
			//Console.Write("val = " + val + " ");

			int[] tBinary = Convert.ToString(EventsSeen, 2).PadLeft(logT, '0').ToCharArray().Select(c => c == '0' ? 0 : 1).ToArray();
			//Console.Write(string.Join("", tBinary.Select(s => s.ToString()).ToArray()));
			int i = logT - 1;
			while (i > 0 && tBinary[i] == 0) i--;
			i = logT - i - 1;
			//Console.Write(" i = " + i);

			partialSums[i] = val;
			for (int j = 0; j < i; j++)
			{
				partialSums[i] += partialSums[j];
				partialSums[j] = 0;
				noisyPartialSums[j] = 0;
			}

			noisyPartialSums[i] = partialSums[i] + Laplace(1.0 / internalEpsilon);

			//Console.WriteLine(" partial sums: " + string.Join(", ", partialSums.Select(n => n.ToString()).ToArray()));

			LastOutput = 0;
			for (int c = 0; c < logT; c++)
			{
				if (tBinary[c] != 0)
					LastOutput += noisyPartialSums[logT - c - 1];
			}


			if (OnOutput != null)
			{
				OnOutput(LastOutput.Value);
			}

			signalEndProcessed();
		}
	}

	public class UserDensity<T> : StreamingUserAlgorithm<T>
	{
		private Dictionary<T, bool> sampled;

		public UserDensity(StreamingQueryable<T> s, double epsilon, List<T> universe, double accuracy, double confidence) 
			: base(s, epsilon)
		{
			if (epsilon > 0.5)
			{
				throw new Exception("epsilon too high");
			}
			double beta = 1.0 - confidence;
			int size = Convert.ToInt32((200 * Math.Log(1.0 / beta, 2)) / (epsilon * epsilon * accuracy * accuracy));
			size = Math.Min (universe.Count, size);
			IEnumerable<T> sample = universe.OrderBy(x => random.Next()).Take(size);
			sampled = new Dictionary<T, bool>(size);
			foreach (T item in sample)
			{
				sampled[item] = sampleInitial();
			}
		}

		public override void EventReceived (T data)
		{
			base.EventReceived (data);

			if (sampled.ContainsKey(data))
			{
				sampled[data] = sampleChosen();
			}
			
			signalEndProcessed();
		}

		public override double GetOutput ()
		{
			StopReceiving();
			if (!LastOutput.HasValue)
				LastOutput = ComputeOutput();

			return LastOutput.Value;
		}

		protected double ComputeOutput()
		{
			double density = sampled.Values.Sum(b => b ? 1 : 0) / (double)sampled.Count;
			return 4 * (density - 0.5) / Epsilon + Laplace(1.0 / (Epsilon * sampled.Count));
		}

		private bool sampleInitial()
		{
			return random.NextDouble() <= 0.5;
		}

		private bool sampleChosen()
		{
			return random.NextDouble() <= 0.5 + (Epsilon/4);
		}

		public int SampleSize { get { return sampled.Count; } }
	}

	public class UserDensityContinuous<T> : UserDensity<T>
	{
		private int k;
		private double d;
		private double rho;
		private int logT;

		private double[] thresholds;
		private double epsilon_threshold;
		private double epsilon_compare;
		private double epsilon_answer;
		private double threshold;
		private int m;

		public UserDensityContinuous(StreamingQueryable<T> s, double epsilon, List<T> universe, double accuracy, double confidence, int maxT) 
			: base(s, epsilon, universe, accuracy, confidence)
		{
			d = 1.0 / universe.Count + 2 * accuracy;
			k = maxT;
			rho = 1.0;
			logT = Convert.ToInt32(Math.Log(maxT, 2) + 0.5);

			epsilon_threshold = rho * logT / epsilon;
			//Console.WriteLine("threshold: " + epsilon_threshold);
			epsilon_compare = 2 * rho * (k + 1) / epsilon;
			epsilon_answer = 2 * rho * (k + 1) / epsilon;
			m = 0;

			threshold = d + 2 * accuracy;

			thresholds = new double[logT];

		}

		public override void EventReceived (T data)
		{
			base.EventReceived(data);
			double lastOutput = LastOutput.GetValueOrDefault(0.0);
			double output = -1;
			double answer = ComputeOutput();


			//I re-randomize every time... hit in accuracy, but still private
			for (int i = 0; i < logT; i++)
			{
				thresholds[i] = Laplace(1.0 / epsilon_threshold);
			}

			double noise_compare = Laplace(1.0 / epsilon_compare);
			double noise_answer = Laplace(1.0 / epsilon_answer);


			double noisy_compare = Math.Abs(lastOutput - answer + noise_compare);
			double thresh_t = threshold + thresholds.Sum();

			//Console.Write ("threshold: " + thresh_t + " diff: " + noisy_compare + " ");

			if (noisy_compare <= thresh_t)
			{
				output = lastOutput;
			}
			else
			{
				output = answer + noise_answer;
				
				if (m < k)
				{
					m++;
				}
				else
				{
					throw new Exception("algorithm not (k,d) varying");
				}
			}

			LastOutput = output;

			
			if (OnOutput != null)
			{
				OnOutput(LastOutput.Value);
			}
			
			signalEndProcessed();
		}
	}
}

