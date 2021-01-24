//X12Feeder_tests.cs
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
using System.Text;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class X12ReadSegment_tests
   {
      [TestMethod]
      public void ReadSegment_NonISA_CorrectSegmentReturned()
      {
         //arrange
         var stream = new MemoryStream(Encoding.UTF8.GetBytes("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~ST*850*000000010"));
         var reader = new StreamReader(stream);
         var feeder = new X12FeederForSource(reader, 0, "~blah");
         var feederPO = new PrivateObject(feeder);

         //act
         var result = feederPO.Invoke("ReadSegment");

         //assert
         result.Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2");
      }


      [TestMethod]
      public void ReadSegment_NonISABadDelimiter_AllDatatReturned()
      {
         //arrange
         var stream = new MemoryStream(Encoding.UTF8.GetBytes("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~ST*850*000000010"));
         var reader = new StreamReader(stream);
         var feeder = new X12FeederForSource(reader, 0, ":blah");
         var feederPO = new PrivateObject(feeder);

         //act
         var result = feederPO.Invoke("ReadSegment");

         //assert
         result.Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~ST*850*000000010");
      }


      [TestMethod]
      public void ReadSegment_NonISALeadingWhitespace_CorrectSegmentReturned()
      {
         //arrange
         var stream = new MemoryStream(Encoding.UTF8.GetBytes(@"
GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~
ST*850*000000010"));
         var reader = new StreamReader(stream);
         var feeder = new X12FeederForSource(reader, 0, "~blah");
         var feederPO = new PrivateObject(feeder);

         //act
         var result = feederPO.Invoke("ReadSegment");

         //assert
         result.Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2");
      }


      [TestMethod]
      public void ReadSegment_ISASimple_CorrectSegmentReturned()
      {
         //arrange
         var stream = new MemoryStream(Encoding.UTF8.GetBytes("ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~"));
         var reader = new StreamReader(stream);
         var feeder = new X12FeederForSource(reader, 0, "blah");
         var feederPO = new PrivateObject(feeder);

         //act
         var result = feederPO.Invoke("ReadSegment");  //as this ISA segment, it will be properly recognized regardless of delimiter value passed to the ctor

         //assert
         result.Should().Be("ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~");  //note that ISA (unlike other segments) includes segment delimiter at end
      }


      [TestMethod]
      public void ReadSegment_ISALeadingWhitespace_CorrectSegmentsReturned()
      {
         //arrange
         var stream = new MemoryStream(Encoding.UTF8.GetBytes(@"
IEA*1*000000905~
ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~
GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS2~
ST*850*000000010"));
         var reader = new StreamReader(stream);
         var feeder = new X12FeederForSource(reader, 0, "~blah");
         var feederPO = new PrivateObject(feeder);

         //act
         var result = feederPO.Invoke("ReadSegment");
         var result2 = feederPO.Invoke("ReadSegment");

         //assert
         result.Should().Be("IEA*1*000000905");
         result2.Should().Be("ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~");  //note that ISA (unlike other segments) includes segment delimiter at end
      }

   }
}
