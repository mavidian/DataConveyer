//LineDispenser_tests.cs
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
using Mavidian.DataConveyer.Output;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataConveyer_tests.Output
{
   [TestClass]
   public class LineDispenser_tests
   {
      //These tests use (up to) 3 targets, i.e. target numbers 1, 2 & 3 (1-based).
      // Note that corresponding indexes in _writers and _sentData are 0, 1 & 2 (0-based).

      private StringWriter[] _writers;     //to be used when constructing TextDisposer
      private StringBuilder[] _sentData;   //data received by corresponding _writers element

      private IEnumerable<string> WrittenLines(int targetNo)
      {
         // lines that were written to a given writer (identified by the target number)
         var reader = new StringReader(_sentData[targetNo-1].ToString());
         string line;
         while (true)
         {
            line = reader.ReadLine();
            if (line == null) yield break;
            yield return line;
         }
      }
      [TestInitialize]
      public void Initialize()
      {
         _sentData = new StringBuilder[3];
         _writers = new StringWriter[3];

         for (int i = 0; i < 3; i++)
         {
            _sentData[i] = new StringBuilder();
            _writers[i] = new StringWriter(_sentData[i]);
         }
      }


      [TestMethod]
      public void SendNextLine_2LinesToSingleTarget_CorrectDataWritten()
      {
         //arrange
         int target1 = 1; 
         var dispenser = LineDispenserCreator.CreateLineDispenser(Enumerable.Repeat(_writers[0], 1), KindOfTextData.Raw, false, null, null);  //only the first of the 3 writers is used in this test

         //act
         dispenser.SendNextLine("first line".ToExternalTuple(target1));
         dispenser.SendNextLine("second line".ToExternalTuple(target1));

         //assert
         var writtenLines = WrittenLines(target1).ToList();
         writtenLines.Count().Should().Be(2);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
      }


      [TestMethod]
      public void SendNextLine_2LinesPlusNullsToSingleTarget_CorrectDataWritten()
      {
         //arrange
         int target1 = 1;
         var dispenser = LineDispenserCreator.CreateLineDispenser(Enumerable.Repeat(_writers[0], 1), KindOfTextData.Raw, false, null, null);  //only the first of the 3 writers is used in this test

         //act
         dispenser.SendNextLine("first line".ToExternalTuple(target1));
         dispenser.SendNextLine("second line".ToExternalTuple(target1));
         dispenser.SendNextLine(string.Empty.ToExternalTuple(target1));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target1));  //null string sent is indistinguishable from empty string
         dispenser.SendNextLine(null);  //end-of-data - this null is not forwarded to any target

         //assert
         var writtenLines = WrittenLines(target1).ToList();
         writtenLines.Count().Should().Be(4);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
         writtenLines[2].Should().Be(string.Empty);
         writtenLines[3].Should().Be(string.Empty);
      }


      [TestMethod]
      public void SendNextLine_VariousLinesToManyTargets_CorrectDataWritten()
      {
         //arrange
         int target1 = 1;
         int target2 = 2;
         int target3 = 3;
         var dispenser = LineDispenserCreator.CreateLineDispenser(new StringWriter[] { _writers[0], _writers[1], _writers[2] }, KindOfTextData.Raw, false, null, null);

         //act
         dispenser.SendNextLine("first line to #3".ToExternalTuple(target3));
         dispenser.SendNextLine("first line to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(string.Empty.ToExternalTuple(target1));
         dispenser.SendNextLine("third line to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target2));
         dispenser.SendNextLine("second line to #2".ToExternalTuple(target2));
         dispenser.SendNextLine("third line to #2".ToExternalTuple(target2));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target1));
         dispenser.SendNextLine("fourth line to #2".ToExternalTuple(target2));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target2));
         dispenser.SendNextLine("sixth line to #2".ToExternalTuple(target2));
         dispenser.SendNextLine(null);  //end-of-data - this null is not forwarded to any target

         //assert
         var writtenLines = WrittenLines(target1).ToList();  //first target
         writtenLines.Count().Should().Be(4);
         writtenLines[0].Should().Be("first line to #1");
         writtenLines[1].Should().Be(string.Empty);
         writtenLines[2].Should().Be("third line to #1");
         writtenLines[3].Should().Be(string.Empty);

         writtenLines = WrittenLines(target2).ToList();  //second target
         writtenLines.Count().Should().Be(6);
         writtenLines[0].Should().Be(string.Empty);
         writtenLines[1].Should().Be("second line to #2");
         writtenLines[2].Should().Be("third line to #2");
         writtenLines[3].Should().Be("fourth line to #2");
         writtenLines[4].Should().Be(string.Empty);
         writtenLines[5].Should().Be("sixth line to #2");

         writtenLines = WrittenLines(target3).ToList();  //third target
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be("first line to #3");
      }


      [TestMethod]
      public void SendNextLine_SegmentsTo2X12Targets_CorrectDataWritten()
      {
         //arrange
         int target1 = 1;
         int target2 = 2;
         var dispenser = LineDispenserCreator.CreateLineDispenser(new StringWriter[] { _writers[0], _writers[1], _writers[2] }, KindOfTextData.X12, false, new Lazy<string>(() => "~"), null);

         //act
         dispenser.SendNextLine("first segment to #2".ToExternalTuple(target2));
         dispenser.SendNextLine("first segment to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(string.Empty.ToExternalTuple(target1));
         dispenser.SendNextLine("third segment to #1".ToExternalTuple(target1));
         dispenser.SendNextLine("second segment to #2".ToExternalTuple(target2));
         dispenser.SendNextLine("third segment to #2".ToExternalTuple(target2));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target1));
         dispenser.SendNextLine("fourth segment to #2".ToExternalTuple(target2));
         dispenser.SendNextLine(null);  //end-of-data - this null is not forwarded to any target

         //assert
         var writtenLines = WrittenLines(target1).ToList();  //first target
         writtenLines.Count().Should().Be(1);  //all segments in a single line
         writtenLines[0].Should().Be("first segment to #1~~third segment to #1~~");

         writtenLines = WrittenLines(target2).ToList();  //second target
         writtenLines.Count().Should().Be(1);  //all segments in a single line
         writtenLines[0].Should().Be("first segment to #2~second segment to #2~third segment to #2~fourth segment to #2~");
      }


      [TestMethod]
      public void SendNextLine_NotAllTargetsUsed_CorrectDataWritten()
      {
         //arrange
         int target1 = 1;
         int target2 = 2;
         int target3 = 3;
         var dispenser = LineDispenserCreator.CreateLineDispenser(new StringWriter[] { _writers[0], _writers[1], _writers[2] }, KindOfTextData.Raw, false, null, null);

         //act
         dispenser.SendNextLine("first line to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(string.Empty.ToExternalTuple(target1));
         dispenser.SendNextLine("third line to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target2));
         dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target1));

         //assert
         var writtenLines = WrittenLines(target1).ToList();  //first target
         writtenLines.Count().Should().Be(4);
         writtenLines[0].Should().Be("first line to #1");
         writtenLines[1].Should().Be(string.Empty);
         writtenLines[2].Should().Be("third line to #1");
         writtenLines[3].Should().Be(string.Empty);

         writtenLines = WrittenLines(target2).ToList();  //second target
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be(string.Empty);

         writtenLines = WrittenLines(target3).ToList();  //third target
         writtenLines.Count().Should().Be(0);
      }


      [TestMethod]
      public void Dispose_Call_MakesDispenserDisposed()
      {
         //arrange
         int target1 = 1;
         int target2 = 2;
         int target3 = 3;
         var dispenser = LineDispenserCreator.CreateLineDispenser(new StringWriter[] { _writers[0], _writers[1], _writers[2] }, KindOfTextData.Raw, false, null, null);

         //act
         dispenser.SendNextLine("first line to #1".ToExternalTuple(target1));
         dispenser.SendNextLine(string.Empty.ToExternalTuple(target3));
         dispenser.Dispose();
         Action a = () => { dispenser.SendNextLine("second line to #1".ToExternalTuple(target1)); };

         //assert
         a.Should().Throw<ObjectDisposedException>().WithMessage("Cannot write to a closed TextWriter.");

         a = () => { dispenser.SendNextLine(Tuple.Create<ExternalLine, int>(null, target2)); };
         a.Should().Throw<ObjectDisposedException>().WithMessage("Cannot write to a closed TextWriter.");

         a = () => { dispenser.SendNextLine("second line to #3".ToExternalTuple(target3)); };
         a.Should().Throw<ObjectDisposedException>().WithMessage("Cannot write to a closed TextWriter.");

         var writtenLines = WrittenLines(target1).ToList();  //first target
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be("first line to #1");

         writtenLines = WrittenLines(target2).ToList();  //second target
         writtenLines.Count().Should().Be(0);

         writtenLines = WrittenLines(target3).ToList();  //third target
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be(string.Empty);
      }


      [TestMethod]
      public void SendNextLine_NonExistingTarget_ThrowsOutOfRangeException()
      {
         //arrange
         int target1 = 1;
         int target2 = 2;
         int target3 = 3;
         int badTarget = 4;  //non-existing
         var dispenser = LineDispenserCreator.CreateLineDispenser(new StringWriter[] { _writers[0], _writers[1], _writers[2] }, KindOfTextData.Raw, false, null, null);

         //act
         dispenser.SendNextLine("first line to #1".ToExternalTuple(target1));
         Action a = () => { dispenser.SendNextLine(string.Empty.ToExternalTuple(badTarget)); };


         //assert
         a.Should().Throw<ArgumentOutOfRangeException>();

         var writtenLines = WrittenLines(target1).ToList();  //first target
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be("first line to #1");

         writtenLines = WrittenLines(target2).ToList();  //second target
         writtenLines.Count().Should().Be(0);

         writtenLines = WrittenLines(target3).ToList();  //third target
         writtenLines.Count().Should().Be(0);
      }

   }
}
