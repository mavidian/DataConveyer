//LineFeederForSource_tests.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Intake;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Linq;
using System.Text;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class LineFeederForSource_tests
   {
      private const string TwoLiner = "First Line\r\nSecond Line\r\n";  //note that CRLF at end is inconsequential (TextReader.ReadLine behavior)
      private const string SingleLine = "The only line";

      private MemoryStream _twoLinerStream;
      private MemoryStream _singleLineStream;
      private MemoryStream _emptyStream;


      [TestInitialize()]
      public void Initialize()
      {
         _twoLinerStream = new MemoryStream(Encoding.UTF8.GetBytes(TwoLiner));
         _singleLineStream = new MemoryStream(Encoding.UTF8.GetBytes(SingleLine));
         _emptyStream = new MemoryStream(Encoding.UTF8.GetBytes(string.Empty));
      }


      // Tests for GetNextLine method.


      [TestMethod]
      public void GetNextLine_TwoLiner_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 6;

         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_twoLinerStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, false, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();
         var fourth = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("First Line");
         first.Item2.Should().Be(srcNo);
         second.Item1.Text.Should().Be("Second Line");
         second.Item2.Should().Be(srcNo);
         third.Should().BeNull();
         fourth.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_TwoLinerSkipHeader_HeaderSkipped()
      {
         //arrange
         const int srcNo = 4;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_twoLinerStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, true, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();
         var fourth = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("Second Line");
         first.Item2.Should().Be(srcNo);
         second.Should().BeNull();
         third.Should().BeNull();
         fourth.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_SingleLine_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 20;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_singleLineStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, false, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("The only line");
         first.Item2.Should().Be(srcNo);
         second.Should().BeNull();
         third.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_SingleLineSkipHeader_HeaderSkipped()
      {
         //arrange
         const int srcNo = 20;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_singleLineStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, true, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Should().BeNull();
         second.Should().BeNull();
         third.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_Empty_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 33;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_emptyStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, false, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Should().BeNull();
         second.Should().BeNull();
         third.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_EmptySkipHeader_NothingToSkip()
      {
         //arrange
         const int srcNo = 33;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_emptyStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, false, true, null, null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Should().BeNull();
         second.Should().BeNull();
         third.Should().BeNull();
      }


      [TestMethod]
      public void GetNextLine_TwoLinerAsEnumerable_CorrectDataReturned()
      {
         // Note that this test actually creates LineFeeder (with a single constituent LineFeederForSource) 

         //arrange
         var feeder = LineFeederCreator.CreateLineFeeder(Enumerable.Repeat(new StreamReader(_twoLinerStream, Encoding.UTF8), 1), KindOfTextData.Raw, false, false, null, null);

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();
         var fourth = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First Line");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second Line");
         second.Item2.Should().Be(1);
         third.Should().BeNull();
         fourth.Should().BeNull();
      }


      // Tests for GetNextLineAsync method.
      // Note that at end of sequence LineFeederForSource returns a tuple containing null and a source number.


      [TestMethod]
      public void GetNextLineAsync_TwoLiner_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 6;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_twoLinerStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, false, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;
         var fourth = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("First Line");
         first.Item2.Should().Be(srcNo);
         second.Item1.Text.Should().Be("Second Line");
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
         fourth.Item1.Should().BeNull();
         fourth.Item2.Should().Be(srcNo);
      }


      [TestMethod]
      public void GetNextLineAsync_TwoLinerSkipHeader_HeaderSkipped()
      {
         //arrange
         const int srcNo = 4;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_twoLinerStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, true, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;
         var fourth = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("Second Line");
         first.Item2.Should().Be(srcNo);
         second.Item1.Should().BeNull();
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
         fourth.Item1.Should().BeNull();
         fourth.Item2.Should().Be(srcNo);
      }



      [TestMethod]
      public void GetNextLineAsync_SingleLine_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 20;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_singleLineStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, false, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Text.Should().Be("The only line");
         first.Item2.Should().Be(srcNo);
         second.Item1.Should().BeNull();
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
      }


      [TestMethod]
      public void GetNextLineAsync_SingleLineSkipHeader_HeaderSkipped()
      {
         //arrange
         const int srcNo = 20;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_singleLineStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, true, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Should().BeNull();
         first.Item2.Should().Be(srcNo);
         second.Item1.Should().BeNull();
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
      }


      [TestMethod]
      public void GetNextLineAsync_Empty_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 33;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_emptyStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, false, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Should().BeNull();
         first.Item2.Should().Be(srcNo);
         second.Item1.Should().BeNull();
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
      }


      [TestMethod]
      public void GetNextLineAsync_EmptySkipHeader_NothingToSkip()
      {
         //arrange
         const int srcNo = 33;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_emptyStream, Encoding.UTF8), srcNo, KindOfTextData.Raw, true, true, null, null });

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<TextFeederForSource>();
         first.Item1.Should().BeNull();
         first.Item2.Should().Be(srcNo);
         second.Item1.Should().BeNull();
         second.Item2.Should().Be(srcNo);
         third.Item1.Should().BeNull();
         third.Item2.Should().Be(srcNo);
      }


      [TestMethod]
      public void GetNextLineAsync_TwoLinerAsEnumerable_CorrectDataReturned()
      {
         // Note that this test actually creates LineFeeder (with a single constituent LineFeederForSource) 

         //arrange
         var feeder = LineFeederCreator.CreateLineFeeder(Enumerable.Repeat(new StreamReader(_twoLinerStream, Encoding.UTF8), 1), KindOfTextData.Raw, true, false, null, null);

         //act
         var first = feeder.GetNextLineAsync().Result;
         var second = feeder.GetNextLineAsync().Result;
         var third = feeder.GetNextLineAsync().Result;
         var fourth = feeder.GetNextLineAsync().Result;

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First Line");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second Line");
         second.Item2.Should().Be(1);
         third.Item1.Should().BeNull();   //tuple with null at end of source
         third.Item2.Should().Be(1);
         fourth.Should().BeNull();        //null tuples thereafter
      }


      [TestMethod]
      public void GetNextLineAsync_TwoLinerAsEnumerableSynced_CorrectDataReturned()
      {
         // Note that this test actually creates LineFeeder (with a single constituent LineFeederForSource) 

         //arrange
         var feeder = LineFeederCreator.CreateLineFeeder(Enumerable.Repeat(new StreamReader(_twoLinerStream, Encoding.UTF8), 1), KindOfTextData.Raw, false, false, null, null);

         //act
         var first = feeder.GetNextLineSynced();
         var second = feeder.GetNextLineSynced();
         var third = feeder.GetNextLineSynced();
         var fourth = feeder.GetNextLineSynced();

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         first.Item1.Text.Should().Be("First Line");
         first.Item2.Should().Be(1);
         second.Item1.Text.Should().Be("Second Line");
         second.Item2.Should().Be(1);
         third.Should().BeNull();
         fourth.Should().BeNull();
      }

   }
}
