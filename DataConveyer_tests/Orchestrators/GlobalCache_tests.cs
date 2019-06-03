//GlobalCache_tests.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using FluentAssertions;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class GlobalCache_tests
   {
      ConcurrentDictionary<string, object> _repo;

      [TestInitialize()]
      public void Initialize()
      {
         _repo = new ConcurrentDictionary<string, object>(new KeyValuePair<string, object>[]
         {
            new KeyValuePair<string, object>("I1", 0),
            new KeyValuePair<string, object>("D1", new DateTime(2010,6,20)),
            new KeyValuePair<string, object>("M1", 20.5m),
            new KeyValuePair<string, object>("S1", "hello"),
            new KeyValuePair<string, object>("S2", null),
            new KeyValuePair<string, object>("S3", "Jane"),
            new KeyValuePair<string, object>("I2", -5),
            new KeyValuePair<string, object>("I3", 3)
        });
      }

      [TestMethod]
      public void GlobalCache_CtorAndIndexer_CorrectData()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         Action badKey = () => Console.Write(gc["NonEx"]);

         //assert
         badKey.Should().Throw<ArgumentOutOfRangeException>().WithMessage("*There is no 'NonEx' element in GlobalCache.*");  //*'s are wildcards

         gc.Count.Should().Be(8);
         gc["I1"].Should().BeOfType<int>();
         gc["I1"].Should().Be(0);
         gc["D1"].Should().BeOfType<DateTime>();
         gc["D1"].Should().Be(new DateTime(2010, 6, 20));
         gc["M1"].Should().BeOfType<decimal>();
         gc["M1"].Should().Be(20.5m);
         gc["S1"].Should().BeOfType<string>();
         gc["S1"].Should().Be("hello");
         gc["S2"].Should().BeNull();  //note the type is not known
         gc["S3"].Should().BeOfType<string>();
         gc["S3"].Should().Be("Jane");
         gc["I2"].Should().BeOfType<int>();
         gc["I2"].Should().Be(-5);
         gc["I3"].Should().BeOfType<int>();
         gc["I3"].Should().Be(3);
      }


      [TestMethod]
      public void GlobalCache_TryGet_CorrectData()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         object val = "dummy";
         var ok = gc.TryGet("NonEx", out val);

         //assert
         ok.Should().BeFalse();
         val.Should().BeNull();

         ok = gc.TryGet("I1", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<int>();
         val.Should().Be(0);
         ok = gc.TryGet("D1", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<DateTime>();
         val.Should().Be(new DateTime(2010, 6, 20));
         ok = gc.TryGet("M1", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<decimal>();
         val.Should().Be(20.5m);
         ok = gc.TryGet("S1", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<string>();
         val.Should().Be("hello");
         ok = gc.TryGet("S2", out val);
         ok.Should().BeTrue();
         val.Should().BeNull();  //the type is not known
         ok = gc.TryGet("S3", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<string>();
         val.Should().Be("Jane");
         ok = gc.TryGet("I2", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<int>();
         val.Should().Be(-5);
         ok = gc.TryGet("I3", out val);
         ok.Should().BeTrue();
         val.Should().BeOfType<int>();
         val.Should().Be(3);
      }


      [TestMethod]
      public void GlobalCache_IncrementByOnMultiThreads_CorrectData()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         var t1 = Task.Run(() => { for (int i = 0; i < 10000; i++) gc.IncrementValue("I1", 1); });
         var t2 = Task.Run(() => { for (int i = 0; i < 8000; i++) gc.IncrementValue("I1", 2); });
         var t3 = Task.Run(() => { for (int i = 0; i < 5001; i++) gc.IncrementValue("I1", -3); });
         Task.WaitAll(t1, t2, t3);

         //assert
         gc["I1"].Should().Be(10997);  //10000 + 2*8000 - 3*5001
      }


      [TestMethod]
      public void GlobalCache_IncrementByBadElement_Throws()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         Action badKey = () => gc.IncrementValue("NonEx", 1);
         Action nonIntKey = () => gc.IncrementValue("D1", 1);

         //assert
         badKey.Should().Throw<ArgumentException>().WithMessage("*There is no 'NonEx' element in GlobalCache.*");  //*'s are wildcards
         nonIntKey.Should().Throw<ArgumentException>().WithMessage("*The GlobalCache element 'D1' is not of integer type.*");  //*'s are wildcards
      }



      [TestMethod]
      public void GlobalCache_TryReplaceSingleAccess_CorrectReplacements()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         var ok = gc.TryReplace("I1", 77, 1);
         Action badKey = () => gc.TryReplace("NonEx", 77, 1);


         //assert
         ok.Should().BeFalse();
         gc["I1"].Should().Be(0);

         ok = gc.TryReplace("I1", 77, 0);
         ok.Should().BeTrue();
         gc["I1"].Should().Be(77);
         ok = gc.TryReplace("I1", 88, 77);
         ok.Should().BeTrue();
         gc["I1"].Should().Be(88);
         ok = gc.TryReplace("D1", new DateTime(2018, 1, 1), new DateTime(2010, 6, 20));
         ok.Should().BeTrue();
         gc["D1"].Should().Be(new DateTime(2018, 1, 1));
         ok = gc.TryReplace("D1", new DateTime(2020, 1, 1), new DateTime(2010, 6, 20));
         ok.Should().BeFalse();
         gc["D1"].Should().Be(new DateTime(2018, 1, 1));
         ok = gc.TryReplace("M1", 1m, 10m);
         ok.Should().BeFalse();
         gc["M1"].Should().Be(20.5m);
         ok = gc.TryReplace("M1", 1m, 20.5m);
         ok.Should().BeTrue();
         gc["M1"].Should().Be(1m);
         ok = gc.TryReplace("S1", "world", "hello");
         ok.Should().BeTrue();
         gc["S1"].Should().Be("world");
         ok = gc.TryReplace("S1", null, "world");
         ok.Should().BeTrue();
         gc["S1"].Should().BeNull();
         ok = gc.TryReplace("S2", "some", null);
         ok.Should().BeTrue();
         gc["S2"].Should().Be("some");
         ok = gc.TryReplace("I2", 100, -5);
         ok.Should().BeTrue();
         gc["I2"].Should().Be(100);

         badKey.Should().Throw<ArgumentOutOfRangeException>().WithMessage("*There is no 'NonEx' element in GlobalCache.*");  //*'s are wildcards
      }


      [TestMethod]
      public void GlobalCache_TryReplaceDualAccess_CorrectReplacements()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);
         bool ok1 = true;
         bool ok2 = false;

         //act
         var t1 = Task.Run(() => { Thread.Sleep(1); ok1 = gc.TryReplace("S3", "Mary", "Jane"); });  //this will fail due to delay
         var t2 = Task.Run(() => { ok2 = gc.TryReplace("S3", "Susan", "Jane"); });
         Task.WaitAll(t1, t2);

         //assert
         ok1.Should().BeFalse();
         ok2.Should().BeTrue();
         gc["S3"].Should().Be("Susan");
      }


      [TestMethod]
      public void GlobalCache_TryReplaceMultiAccess_CorrectReplacements()
      {
         //Note the following pattern for replacing GlobalCache values using TryReplace in a thread-safe manner:
         //       T prevVal; //, where T is one of: bool, int, DateTime, decimal or string
         //       do { prevVal = (T)gc["key"]; }
         //       while (!gc.TryReplace("key", CalcNewVal(prevVal), prevVal));

         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);
         int attemptCnt = 0;

         //act
         var t1 = Task.Run(() =>
         {
            for (int i = 0; i < 500; i++)
            {
               decimal num;
               do
               {
                  num = (decimal)gc["M1"];
                  Interlocked.Increment(ref attemptCnt);
               }
               while (!gc.TryReplace("M1", num + .5m, num));
            }
         });
         var t2 = Task.Run(() =>
         {
            for (int i = 0; i < 600; i++)
            {
               decimal num;
               do
               {
                  num = (decimal)gc["M1"];
                  Interlocked.Increment(ref attemptCnt);
               }
               while (!gc.TryReplace("M1", num + .33m, num));
            }
         });
         Task.WaitAll(t1, t2);

         //assert
         gc["M1"].Should().Be(468.5m);  //20.5 + 500*.5 + 600*.33
         attemptCnt.Should().BeGreaterOrEqualTo(1100);  //500 + 600 + likely retries
      }


      [TestMethod]
      public void GlobalCache_ReplacValueeMultiAccess_CorrectReplacements()
      {
         //arrange
         IGlobalCache gc = new GlobalCache(_repo, 30);

         //act
         var t1 = Task.Run(() =>
         {
            for (int i = 0; i < 1100; i++) { gc.ReplaceValue<decimal, decimal>("M1", n => n + .33m ); }
         });
         var t2 = Task.Run(() =>
         {
            for (int i = 0; i < 901; i++) { gc.ReplaceValue<decimal, decimal>("M1", n => n + .5m); }
         });
         var t3 = Task.Run(() =>
         {
            for (int i = 0; i < 962; i++) { gc.ReplaceValue<DateTime, DateTime>("D1", d => d.AddMonths(1)); }
         });
         var t4 = Task.Run(() =>
         {
            for (int i = 0; i < 600; i++) { gc.ReplaceValue<DateTime, DateTime>("D1", d => d.AddYears(2)); }
         });
         Task.WaitAll(t1, t2, t3, t4);
         gc.ReplaceValue<decimal, string>("M1", n => n.ToString());

         //assert
         gc["M1"].Should().Be("834.00");  //20.5 + 1100*.33 + 901*.5
         gc["D1"].Should().Be(new DateTime(3290, 8, 20));  //6/20/2010 + 962mos + 1200yrs
      }

   }
}