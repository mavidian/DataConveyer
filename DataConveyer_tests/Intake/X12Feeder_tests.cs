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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace DataConveyer_tests.Intake
{
   [TestClass]
   public class X12Feeder_tests
   {
      private const string X12WOLines = "ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~GS*HS*AV01101957*030240928*20031023*17581205*1*X*004010X092A1~ST*270*0001~BHT*0022*13*19637886*20031023*17581205~HL*1**20*1~NM1*PR*2*HUMANA*****PI*HUMANA~HL*2*1*21*1~NM1*1P*2*Hospital Name*****FI*111111111~HL*3*2*22*0~TRN*1*19637886*3030240928~NM1*IL*1*LastName*FirstName****MI*1111111111~DMG*D8*20000101*F~DTP*472*D8*20031023~EQ*30~SE*13*0001~GE*1*1~IEA*1*557988899~";
      private const string X12WithLines = @"ISA*00*          *01*SECRET    *ZZ*SUBMITTERS.ID  *ZZ*RECEIVERS.ID   *030101*1253*^*00501*000000905*1*T*:~
GS*HC*SENDER CODE*RECEIVER CODE*19991231*0802*1*X*005010X222~
ST*837*0021*005010X222~
BHT*0019*00*244579*20061015*1023*CH~
NM1*41*2*PREMIER BILLING SERVICE*****46*TGJ23~
PER*IC*JERRY*TE*3055552222*EX*231~
NM1*40*2*KEY INSURANCE COMPANY*****46*66783JJT~
HL*1**20*1~
PRV*BI*PXC*203BF0100Y~
NM1*85*2*BEN KILDARE SERVICE*****XX*9876543210~
N3*234 SEAWAY ST~
N4*MIAMI*FL*33111~
REF*EI*587654321~
NM1*87*2~
N3*2345 OCEAN BLVD~
N4*MAIMI*FL*33111~
HL*2*1*22*1~
SBR*P**2222-SJ******CI~
NM1*IL*1*SMITH*JANE****MI*JS00111223333~
DMG*D8*19430501*F~
NM1*PR*2*KEY INSURANCE COMPANY*****PI*999996666~
REF*G2*KA6663~
HL*3*2*23*0~
PAT*19~
NM1*QC*1*SMITH*TED~
N3*236 N MAIN ST~
N4*MIAMI*FL*33413~
DMG*D8*19730501*M~
CLM*26463774*100***11:B:1*Y*A*Y*I~
REF*D9*17312345600006351~
HI*BK:0340*BF:V7389~
LX*1~
SV1*HC:99213*40*UN*1***1~
DTP*472*D8*20061003~
LX*2~
SV1*HC:87070*15*UN*1***1~
DTP*472*D8*20061003~
LX*3~
SV1*HC:99214*35*UN*1***2~
DTP*472*D8*20061010~
LX*4~
SV1*HC:86663*10*UN*1***2~
DTP*472*D8*20061010~
SE*42*0021~
GE*1*1~
IEA*1*000000905~";

      private const string X12LineAsDelimiter = @"ISA*01*0000000000*01*0000000000*ZZ*ABCDEFGHIJKLMNO*ZZ*123456789012345*101127*1719*U*00400*000003438*0*P*>
GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS
ST*850*000000010
BEG*00*SA*08292233294**20101127*610385385
REF*DP*038
REF*PS*R
ITD*14*3*2**45**46
DTM*002*20101214
PKG*F*68***PALLETIZE SHIPMENT
PKG*F*66***REGULAR
TD5*A*92*P3**SEE XYZ RETAIL ROUTING GUIDE
N1*ST*XYZ RETAIL*9*0003947268292
N3*31875 SOLON RD
N4*SOLON*OH*44139
PO1*1*120*EA*9.25*TE*CB*065322-117*PR*RO*VN*AB3542
PID*F****SMALL WIDGET
PO4*4*4*EA*PLT94**3*LR*15*CT
PO1*2*220*EA*13.79*TE*CB*066850-116*PR*RO*VN*RD5322
PID*F****MEDIUM WIDGET
PO4*2*2*EA
PO1*3*126*EA*10.99*TE*CB*060733-110*PR*RO*VN*XY5266
PID*F****LARGE WIDGET
PO4*6*1*EA*PLT94**3*LR*12*CT
PO1*4*76*EA*4.35*TE*CB*065308-116*PR*RO*VN*VX2332
PID*F****NANO WIDGET
PO4*4*4*EA*PLT94**6*LR*19*CT
PO1*5*72*EA*7.5*TE*CB*065374-118*PR*RO*VN*RV0524
PID*F****BLUE WIDGET
PO4*4*4*EA
PO1*6*696*EA*9.55*TE*CB*067504-118*PR*RO*VN*DX1875
PID*F****ORANGE WIDGET
PO4*6*6*EA*PLT94**3*LR*10*CT
CTT*6
AMT*1*13045.94
SE*33*000000010
GE*1*1421
IEA*1*000003438";

      private MemoryStream _x12StreamWOLines;
      private MemoryStream _x12StreamWithLines;
      private MemoryStream _x12StreamLineAsDelimiter;
      private MemoryStream _emptyX12Stream;


      [TestInitialize()]
      public void Initialize()
      {
         _x12StreamWOLines = new MemoryStream(Encoding.UTF8.GetBytes(X12WOLines));
         _x12StreamWithLines = new MemoryStream(Encoding.UTF8.GetBytes(X12WithLines));
         _x12StreamLineAsDelimiter = new MemoryStream(Encoding.UTF8.GetBytes(X12LineAsDelimiter));
         _emptyX12Stream = new MemoryStream(Encoding.UTF8.GetBytes(string.Empty));
      }


      // Tests for GetNextLine method, which in case of X12 returns segments


      [TestMethod]
      public void GetNextLine_X12WOLines_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 1;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_x12StreamWOLines, Encoding.UTF8), srcNo, KindOfTextData.X12, false, false, "x", null });  //segment delimiter can be anything, but null here
         var tuplesFed = new List<Tuple<ExternalLine, int>>();

         //act
         var tupleFed = feeder.GetNextLine();
         while (tupleFed != null)
         {
            tuplesFed.Add(tupleFed);
            tupleFed = feeder.GetNextLine();
         }

         //assert
         feeder.Should().BeOfType<X12FeederForSource>();
         //note that segment delimiters are stripped out of segments
         tuplesFed.Count.Should().Be(17);
         tuplesFed[0].Item1.Text.Should().Be("ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~");
         tuplesFed[0].Item2.Should().Be(1);
         tuplesFed[1].Item1.Text.Should().Be("GS*HS*AV01101957*030240928*20031023*17581205*1*X*004010X092A1");
         tuplesFed[1].Item2.Should().Be(1);
         tuplesFed[2].Item1.Text.Should().Be("ST*270*0001");
         tuplesFed[2].Item2.Should().Be(1);
         tuplesFed[3].Item1.Text.Should().Be("BHT*0022*13*19637886*20031023*17581205");
         tuplesFed[3].Item2.Should().Be(1);
         tuplesFed[9].Item1.Text.Should().Be("TRN*1*19637886*3030240928");
         tuplesFed[9].Item2.Should().Be(1);
         tuplesFed[16].Item1.Text.Should().Be("IEA*1*557988899");
         tuplesFed[16].Item2.Should().Be(1);
      }


      [TestMethod]
      public void GetNextLine_X12WithLines_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 2;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_x12StreamWithLines, Encoding.UTF8), srcNo, KindOfTextData.X12, false, false, "#", null });  //segment delimiter can be anything, but null here
         var tuplesFed = new List<Tuple<ExternalLine, int>>();

         //act
         var tupleFed = feeder.GetNextLine();
         while (tupleFed != null)
         {
            tuplesFed.Add(tupleFed);
            tupleFed = feeder.GetNextLine();
         }

         //assert
         feeder.Should().BeOfType<X12FeederForSource>();
         //note that segment delimiters are stripped out of segments
         tuplesFed.Count.Should().Be(46);
         tuplesFed[0].Item1.Text.Should().Be("ISA*00*          *01*SECRET    *ZZ*SUBMITTERS.ID  *ZZ*RECEIVERS.ID   *030101*1253*^*00501*000000905*1*T*:~");
         tuplesFed[0].Item2.Should().Be(2);
         tuplesFed[1].Item1.Text.Should().Be("GS*HC*SENDER CODE*RECEIVER CODE*19991231*0802*1*X*005010X222");
         tuplesFed[1].Item2.Should().Be(2);
         tuplesFed[2].Item1.Text.Should().Be("ST*837*0021*005010X222");
         tuplesFed[2].Item2.Should().Be(2);
         tuplesFed[3].Item1.Text.Should().Be("BHT*0019*00*244579*20061015*1023*CH");
         tuplesFed[3].Item2.Should().Be(2);
         tuplesFed[6].Item1.Text.Should().Be("NM1*40*2*KEY INSURANCE COMPANY*****46*66783JJT");
         tuplesFed[6].Item2.Should().Be(2);
         tuplesFed[11].Item1.Text.Should().Be("N4*MIAMI*FL*33111");
         tuplesFed[11].Item2.Should().Be(2);
         tuplesFed[15].Item1.Text.Should().Be("N4*MAIMI*FL*33111");
         tuplesFed[15].Item2.Should().Be(2);
         tuplesFed[18].Item1.Text.Should().Be("NM1*IL*1*SMITH*JANE****MI*JS00111223333");
         tuplesFed[18].Item2.Should().Be(2);
         tuplesFed[19].Item1.Text.Should().Be("DMG*D8*19430501*F");
         tuplesFed[19].Item2.Should().Be(2);
         tuplesFed[21].Item1.Text.Should().Be("REF*G2*KA6663");
         tuplesFed[21].Item2.Should().Be(2);
         tuplesFed[26].Item1.Text.Should().Be("N4*MIAMI*FL*33413");
         tuplesFed[26].Item2.Should().Be(2);
         tuplesFed[31].Item1.Text.Should().Be("LX*1");
         tuplesFed[31].Item2.Should().Be(2);
         tuplesFed[34].Item1.Text.Should().Be("LX*2");
         tuplesFed[34].Item2.Should().Be(2);
         tuplesFed[41].Item1.Text.Should().Be("SV1*HC:86663*10*UN*1***2");
         tuplesFed[41].Item2.Should().Be(2);
         tuplesFed[43].Item1.Text.Should().Be("SE*42*0021");
         tuplesFed[43].Item2.Should().Be(2);
         tuplesFed[44].Item1.Text.Should().Be("GE*1*1");
         tuplesFed[44].Item2.Should().Be(2);
         tuplesFed[45].Item1.Text.Should().Be("IEA*1*000000905");
         tuplesFed[45].Item2.Should().Be(2);
      }


      [TestMethod]
      public void GetNextLine_X12LineAsDelimiter_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 3;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_x12StreamLineAsDelimiter, Encoding.UTF8), srcNo, KindOfTextData.X12, false, false, "*", null });  //segment delimiter can be anything, but null here
         var tuplesFed = new List<Tuple<ExternalLine, int>>();

         //act
         var tupleFed = feeder.GetNextLine();
         while (tupleFed != null)
         {
            tuplesFed.Add(tupleFed);
            tupleFed = feeder.GetNextLine();
         }

         //assert
         feeder.Should().BeOfType<X12FeederForSource>();
         //note that segment delimiters are stripped out of segments
         tuplesFed.Count.Should().Be(37);
         tuplesFed[0].Item1.Text.Should().Be("ISA*01*0000000000*01*0000000000*ZZ*ABCDEFGHIJKLMNO*ZZ*123456789012345*101127*1719*U*00400*000003438*0*P*>\r");
         tuplesFed[0].Item2.Should().Be(3);
         tuplesFed[1].Item1.Text.Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS");
         tuplesFed[1].Item2.Should().Be(3);
         tuplesFed[2].Item1.Text.Should().Be("ST*850*000000010");
         tuplesFed[2].Item2.Should().Be(3);
         tuplesFed[4].Item1.Text.Should().Be("REF*DP*038");
         tuplesFed[4].Item2.Should().Be(3);
         tuplesFed[7].Item1.Text.Should().Be("DTM*002*20101214");
         tuplesFed[7].Item2.Should().Be(3);
         tuplesFed[9].Item1.Text.Should().Be("PKG*F*66***REGULAR");
         tuplesFed[9].Item2.Should().Be(3);
         tuplesFed[12].Item1.Text.Should().Be("N3*31875 SOLON RD");
         tuplesFed[12].Item2.Should().Be(3);
         tuplesFed[17].Item1.Text.Should().Be("PO1*2*220*EA*13.79*TE*CB*066850-116*PR*RO*VN*RD5322");
         tuplesFed[17].Item2.Should().Be(3);
         tuplesFed[20].Item1.Text.Should().Be("PO1*3*126*EA*10.99*TE*CB*060733-110*PR*RO*VN*XY5266");
         tuplesFed[20].Item2.Should().Be(3);
         tuplesFed[21].Item1.Text.Should().Be("PID*F****LARGE WIDGET");
         tuplesFed[21].Item2.Should().Be(3);
         tuplesFed[26].Item1.Text.Should().Be("PO1*5*72*EA*7.5*TE*CB*065374-118*PR*RO*VN*RV0524");
         tuplesFed[26].Item2.Should().Be(3);
         tuplesFed[28].Item1.Text.Should().Be("PO4*4*4*EA");
         tuplesFed[28].Item2.Should().Be(3);
         tuplesFed[32].Item1.Text.Should().Be("CTT*6");
         tuplesFed[32].Item2.Should().Be(3);
         tuplesFed[36].Item1.Text.Should().Be("IEA*1*000003438");
         tuplesFed[36].Item2.Should().Be(3);
      }


      [TestMethod]
      public void GetNextLine_Empty_CorrectDataReturned()
      {
         //arrange
         const int srcNo = 33;
         var LineFeederCreatePT = new PrivateType(typeof(LineFeederCreator));
         var feeder = (LineFeederForSource)LineFeederCreatePT.InvokeStatic("CreateLineFeeder", new object[] { new StreamReader(_emptyX12Stream, Encoding.UTF8), srcNo, KindOfTextData.X12, false, false, "dummy", null });

         //act
         var first = feeder.GetNextLine();
         var second = feeder.GetNextLine();
         var third = feeder.GetNextLine();

         //assert
         feeder.Should().BeOfType<X12FeederForSource>();
         first.Should().BeNull();
         second.Should().BeNull();
         third.Should().BeNull();
      }

      [TestMethod]
      public void GetNextLine_MultiSources_CorrectDataReturned()
      {
         //arrange
         var streams = new MemoryStream[] { _x12StreamWOLines, _x12StreamLineAsDelimiter, _x12StreamWithLines };
         var feeder = LineFeederCreator.CreateLineFeeder(streams.Select(s => new StreamReader(s, Encoding.UTF8)), KindOfTextData.X12, false, false, "*", null);  //segment delimiter can be anything, but null here
         var tuplesFed = new List<Tuple<ExternalLine, int>>();

         //act
         var tupleFed = feeder.GetNextLine();
         while (tupleFed != null)
         {
            tuplesFed.Add(tupleFed);
            tupleFed = feeder.GetNextLine();
         }

         //assert
         feeder.Should().BeOfType<LineFeeder>();
         tuplesFed.Count.Should().Be(100);  // 17 + 37 + 46
         tuplesFed[0].Item1.Text.Should().Be("ISA*00*          *00*          *ZZ*AV09311993     *01*030240928      *031023*1758*U*00401*557988899*1*T*:~");
         tuplesFed[0].Item2.Should().Be(1);
         tuplesFed[16].Item1.Text.Should().Be("IEA*1*557988899");
         tuplesFed[16].Item2.Should().Be(1);
         tuplesFed[17].Item1.Text.Should().Be("ISA*01*0000000000*01*0000000000*ZZ*ABCDEFGHIJKLMNO*ZZ*123456789012345*101127*1719*U*00400*000003438*0*P*>\r");
         tuplesFed[17].Item2.Should().Be(2);
         tuplesFed[53].Item1.Text.Should().Be("IEA*1*000003438");
         tuplesFed[53].Item2.Should().Be(2);
         tuplesFed[54].Item1.Text.Should().Be("ISA*00*          *01*SECRET    *ZZ*SUBMITTERS.ID  *ZZ*RECEIVERS.ID   *030101*1253*^*00501*000000905*1*T*:~");
         tuplesFed[54].Item2.Should().Be(3);
         tuplesFed[99].Item1.Text.Should().Be("IEA*1*000000905");
         tuplesFed[99].Item2.Should().Be(3);
      }
   }
}