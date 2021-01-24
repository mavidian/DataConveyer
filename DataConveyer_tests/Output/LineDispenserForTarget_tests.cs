//LineDispenserForTarget_tests.cs
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
   public class LineDispenserForTarget_tests
   {
      private StringWriter _writer;     //to be used when constructing TextDisposerTarget
      private StringBuilder _sentData;  //data received by _writer

      private IEnumerable<string> WrittenLines
      {
         // lines that were written to the writer
         get
         {
            var reader = new StringReader(_sentData.ToString());
            string line;
            while (true)
            {
               line = reader.ReadLine();
               if (line == null) yield break;
               yield return line;
            }
         }
      }

      [TestInitialize]
      public void Initialize()
      {
         _sentData = new StringBuilder();
         _writer = new StringWriter(_sentData);
      }


      [TestMethod]
      public void SendNextLine_2Calls_2LinesWritten()
      {
         //arrange
         const int targetNo = 4;
         var dispenser = new TextDispenserForTarget(_writer, targetNo);
         var dispenserPO = new PrivateObject(dispenser);

         //act
         dispenser.SendNextLine("first line".ToExternalTuple(targetNo));
         dispenser.SendNextLine("second line".ToExternalTuple(targetNo));

         //assert
         dispenserPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(2);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
      }


      [TestMethod]
      public void SendNextLine_2CallsX12_2SegmentsWritten()
      {
         //arrange
         const int targetNo = 4;
         var target = new X12DispenserForTarget(_writer, targetNo, new Lazy<string>(() => "~"));  //X12 segment terminator is ~
         var targetPO = new PrivateObject(target);

         //act
         target.SendNextLine("first line".ToExternalTuple(targetNo));
         target.SendNextLine("second line".ToExternalTuple(targetNo));

         //assert
         targetPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(1);  //both segments are in a single line
         writtenLines[0].Should().Be("first line~second line~");
      }


      [TestMethod]
      public void SendNextLine_2CallsX12WithNewLines_2SegmentsWritten()
      {
         //arrange
         const int targetNo = 4;
         var target = new X12DispenserForTarget(_writer, targetNo, new Lazy<string>(() => "\r\n"));  //X12 segment terminator is CRLF (in essence the same as non-X12)
         var targetPO = new PrivateObject(target);

         //act
         target.SendNextLine("first line".ToExternalTuple(targetNo));
         target.SendNextLine("second line".ToExternalTuple(targetNo));

         //assert
         targetPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(2);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
      }


      [TestMethod]
      public void SendNextLine_2CallsPlusNulls_AllLinesWritten()
      {
         //note that null/empty strings are not ignored here (end-of-data marks are handled in LineDispenser (and besides they are null Tuples and not Tuples with empty lines)

         //arrange
         const int targetNo = 11;
         var target = new TextDispenserForTarget(_writer, targetNo);
         var targetPO = new PrivateObject(target);

         //act
         target.SendNextLine("first line".ToExternalTuple(targetNo));  //note that targetNo sent to a LineDispenserForTarget here is ignored
         target.SendNextLine("second line".ToExternalTuple(targetNo));
         target.SendNextLine(string.Empty.ToExternalTuple(targetNo));
         target.SendNextLine(new Tuple<ExternalLine, int>(null, targetNo));

         //assert
         targetPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(4);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
         writtenLines[2].Should().Be(string.Empty);
         writtenLines[3].Should().Be(string.Empty);  //note that null string sent to TextWriter is indistinguishable from empty string
      }


      [TestMethod]
      public void SendNextLineAsync_2CallsPlusNulls_AllLinesWritten()
      {
         //note that null/empty strings are not ignored here (end-of-data marks are "swallowed" before, i.e. in LineDispenser)

         //arrange
         const int targetNo = 11;
         var target = new TextDispenserForTarget(_writer, targetNo);
         var targetPO = new PrivateObject(target);

         //act
         target.SendNextLineAsync("first line".ToExternalTuple(targetNo)).Wait();
         target.SendNextLineAsync("second line".ToExternalTuple(targetNo)).Wait();
         target.SendNextLineAsync(string.Empty.ToExternalTuple(targetNo)).Wait();
         target.SendNextLineAsync(new Tuple<ExternalLine,int>(null, targetNo)).Wait();

         //assert
         targetPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(4);
         writtenLines[0].Should().Be("first line");
         writtenLines[1].Should().Be("second line");
         writtenLines[2].Should().Be(string.Empty);
         writtenLines[3].Should().Be(string.Empty);  //note that null string sent to TextWriter is indistinguishable from empty string
      }


      [TestMethod]
      public void Dispose_Call_MakesTargetDisposed()
      {
         //arrange
         const int targetNo = 9;
         var target = new TextDispenserForTarget(_writer, targetNo);
         var targetPO = new PrivateObject(target);

         //act
         target.SendNextLine("first line".ToExternalTuple(targetNo));
         target.Dispose();
         Action a = () => { target.SendNextLine("second line".ToExternalTuple(targetNo)); };

         //assert
         a.Should().Throw<ObjectDisposedException>().WithMessage("Cannot write to a closed TextWriter.");

         targetPO.GetField("TargetNo").Should().Be(targetNo);
         var writtenLines = WrittenLines.ToList();
         writtenLines.Count().Should().Be(1);
         writtenLines[0].Should().Be("first line");

      }
   }
}
