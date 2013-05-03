using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using PINQ;
using PINQ.Streaming;
using System.IO;

namespace TestHarness
{
    public class TestHarness
    {
        public class PINQAgentLogger : PINQAgent
        {
            string name;
            double total;
            public override bool apply(double epsilon)
            {
                total += epsilon;
                Console.WriteLine("**privacy change**\tdelta: " + epsilon.ToString("0.00") + "\ttotal: " + total.ToString("0.00") + "\t(" + name + ")");
                //Console.WriteLine("**privacy change**\tdelta: {0:F2}\ttotal: {1:F2}\t({2})", epsilon, total, name);

                return true;
            }

            public PINQAgentLogger(string n) { name = n; }
        }

        static void Main(string[] args)
        {

			//var privateNumbers = new RandomNumbers(0, 1, 1); //.filter(n => n > 5).map(n => n*2);
			//var numbers = new StreamingQueryable<double>(privateNumbers, new PINQEventLevelAgent(100.0));
			//var overHalf = numbers.Where(x => x >= 0.5);
			//var constantYes = new FunctionStream<int>(1, i => (i % 2) == 1 ? 1 : 0);
			//var constant = new StreamingQueryable<int>(constantYes, new PINQEventLevelAgent(100.0));

//			var avg = numbers.NoisyAverage(1.0, x => x);
//			avg.ProcessEvents(50);
//			Console.WriteLine("Got noisy average after " + avg.EventsSeen + " events seen: " + avg.GetOutput());

//			var count = constant.RandomizedResponseCount(1.0);
//			//count.OnOutput = (countSoFar => Console.WriteLine("Count: " + countSoFar + " Events seen: " + count.EventsSeen));
//			count.ProcessEvents(2000);
//			Console.WriteLine("Got " + count.GetOutput() + " after seeing " + count.EventsSeen + " events");
//
//
//			var betterCount = constant.BinaryCount(1.0, 2000);
//			betterCount.OnOutput = (countSoFar => Console.WriteLine("Count: " + countSoFar + " Events seen: " + betterCount.EventsSeen));
//			betterCount.ProcessEvents(2000);
//			Console.WriteLine("Got " + betterCount.GetOutput() + " after seeing " + betterCount.EventsSeen + " events");
//
//			privateNumbers.Stop();
//			constantYes.Stop();

//			var mod100 = new FunctionStream<int>(1, i => i % 99 + 1);
//			var stream = new StreamingQueryable<int>(mod100, new PINQUserLevelAgentBudget(3.0));
//			var events = stream.RandomizedResponseCount(0.5);
//			events.ProcessEvents(4, true);
//			Console.WriteLine("processed " + events.EventsSeen);
//
//
//			var alg = stream.UserDensityContinuous(0.5, Enumerable.Range(1, 1000).ToList(), 0.01, 2000);
//			Console.WriteLine("Sample size: " + alg.SampleSize);
//			alg.OnOutput = (density => Console.WriteLine("seen: " + alg.EventsSeen + " density so far: " + density));
//			alg.ProcessEvents(2003);
//			Console.WriteLine("User density: " + alg.GetOutput());
//
//			mod100.Stop();



			var source = new FunctionStream<int>(1, i => i % 10);
			var zeroToNine = new StreamingQueryable<int>(source, new PINQEventLevelAgentBudget(3.0));
			var buckets = zeroToNine.Partition(Enumerable.Range(0, 10).ToArray(), (i => i % 10));
			var countsOnOne = buckets[1].BinaryCount(0.5, 10000);
			var countsOnTwo = buckets[2].RandomizedResponseCount(0.5);
			countsOnOne.OnOutput = cnt => Console.WriteLine("Events Seen: " + countsOnOne.EventsSeen + " Count for 1: " + cnt);
			countsOnTwo.OnOutput = cnt => Console.WriteLine("Events Seen: " + countsOnTwo.EventsSeen + " Count for 2: " + cnt);
			
			countsOnOne.StartReceiving();
			countsOnTwo.ProcessEvents(10000);
			countsOnTwo.StopReceiving();


//

            // preparing a private data source
//            var filename = @"../../TestHarness.cs";
//            var data = File.ReadAllLines(filename).AsQueryable();
//            var text = new PINQueryable<string>(data, new PINQAgentLogger(filename));
//
//            /**** Data is now sealed up. Use from this point on is unrestricted ****/
//
//            // output a noisy count of the number of lines of text
//
//            Console.WriteLine("Lines of text: " + text.NoisyCount(1.0));
//			Console.WriteLine("Lines of text (2): " + text.NoisyCount(1.0));
//
//            // restrict using a user defined predicate, and count again (with noise)
//            Console.WriteLine("Lines with semi-colons: " + text.Where(line => line.Contains(';')).NoisyCount(1.0));
//
//            // think about splitting the records into arrays (declarative, so nothing happens yet)
//            var words = text.Select(line => line.Split(' '));
//
//            // partition the data by number of "words", and count how many of each type there are
//            var keys = new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
//            var parts = words.Partition(keys, line => line.Count());
//            foreach (var count in keys)
//                Console.WriteLine("Lines with " + count + " words:" + "\t" + parts[count].NoisyCount(0.1));
//
//			Console.WriteLine("Super specific query: " + text.Where(line => line.Contains("var words = text.Select(line => line.Split(' '));")).NoisyCount(1.0));
//
//            Console.ReadKey();
       }
    }
}
