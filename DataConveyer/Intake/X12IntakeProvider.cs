//X12IntakeProvider.cs
//
// Copyright © 2017-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;

namespace Mavidian.DataConveyer.Intake
{
   internal class X12IntakeProvider : IntakeProvider
   {
      private readonly X12Delimiters _x12DelimitersForOutput;
      private char _x12FieldDelimiter;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      /// <param name="x12DelimitersForOuput"></param>
      internal X12IntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions, X12Delimiters x12DelimitersForOuput)
         : base(config, globalCache, typeDefinitions, sNo => sNo == 0 ? "Segment" : string.Format("Elem{0:000}", sNo))  //in case of X12, fields are named: Segment, Elem001, Elem002,...
      {
         _x12DelimitersForOutput = x12DelimitersForOuput;
         _x12FieldDelimiter = config.DefaultX12FieldDelimiter;
         if (_x12FieldDelimiter == default(char)) _x12FieldDelimiter = '*';
      }


      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            //In case of X12 data, line means the segment
            return line =>
            {
               var segment = line.Text;
               //Note that this approach allows different delimiters in case of multiple interchange envelopes in a single intake stream,
               if (segment.StartsWith("ISA"))
               {
                  if (segment.Length < 105) throw new ArgumentException($"X12 ISA segment is too short ({segment.Length} chars)."); //exception to be caught in LineParsingBlock ("tokenizer in line parser")
                  _x12FieldDelimiter = segment[3];  //TODO: Verify if Substring performs faster
                  if (_x12DelimitersForOutput.X12FieldDelimiter == default(char)) _x12DelimitersForOutput.X12FieldDelimiter = _x12FieldDelimiter;  //delimiter from only the 1st ISA segment can possibly become used in output
                  //note that ISA segment (unlike other segments) contains segment delimiter at end (unless fed by custom intake supplier, in which case it may not be there (\r\n assumed then))
                  if (_x12DelimitersForOutput.X12SegmentDelimiter == null)
                  {  //in case of multiple ISA segments, delimiter from only the 1st ISA segment can become used in output
                     _x12DelimitersForOutput.X12SegmentDelimiter = segment.Length < 106 ? Environment.NewLine : segment[105].ToString();
                  }
                  segment = segment.Substring(0, 105); //remove segment delimiter
               }
               return segment.Split(_x12FieldDelimiter);
            };
         }
      }

      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) => { return ItemFromTextToken(t, i, base._typeDefinitions); };
         }
      }

      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("X12IntakeProvider must not use ItemFromExtItem function.");

   }
}
