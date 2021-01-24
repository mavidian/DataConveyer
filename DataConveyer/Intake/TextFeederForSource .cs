//TextFeederForSource.cs
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
using System;
using System.IO;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Reader from a single text source. This feeder is common to all line-based feeders, i.e. those feeders that return Xtext.
   /// </summary>
   internal class TextFeederForSource : LineFeederForSource
   {
      //Even though this class implements ILineFeeder, it is not used directly; instead, it only acts as a constituent feeder.

      private readonly bool _skipHeader;
      private bool _atStart;  //reading has not yet started

      /// <summary>
      /// Creates an instance given a stream reader (from a specific text file) a source number and a boolean flag to skip first row.
      /// </summary>
      /// <param name="reader"></param>
      /// <param name="sourceNo">1 based source number.</param>
      /// <param name="skipHeader">If set (true) then first row received from reader gets discarded.</param>
      internal TextFeederForSource(TextReader reader, int sourceNo, bool skipHeader) : base(reader, sourceNo)
      {
         _skipHeader = skipHeader;
         _atStart = true;
      }


      public override Tuple<ExternalLine, int> GetNextLine()
      {
         var line = _reader.ReadLine();
         //Note of TextReader.ReadLine behavior at EOF:
         //     Last line unless empty, does not have to be terminated by NewLine (\r\n).
         //     For example, if a file contains SingleLine, it will be treated the same way as SingleLine\r\n;
         //     in either case, 1st ReadLine returns SingleLine and the second one null.
         if (line == null) return null;
         if (_atStart)
         {
            _atStart = false;  //not thread-safe, but single-threaded
            if (_skipHeader) return GetNextLine();  //header row gets discarded
         }
         return line.ToExternalTuple(_sourceNo);
      }


      public override async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         var line = await _reader.ReadLineAsync();
         if (line == null) return new Tuple<ExternalLine, int>(null, _sourceNo);  //return Tuple<null to indicate end of source (that can be differentiated from null at the end of all sources)
         if (_atStart)
         {
            _atStart = false;  //not thread-safe, but single-threaded
            if (_skipHeader) return await GetNextLineAsync();  //header row gets discarded
         }
         return line.ToExternalTuple(_sourceNo);
      }

   }
}
