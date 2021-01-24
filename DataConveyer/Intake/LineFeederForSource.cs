//LineFeederForSource.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Base reader from a single source (constituent feeder).
   /// </summary>
   internal abstract class LineFeederForSource : IDisposable
   {
      protected readonly TextReader _reader;
      protected readonly int _sourceNo;

      /// <summary>
      /// Creates an instance given a stream reader (from a specific text file) a source number and a boolean flag to skip first row.
      /// </summary>
      /// <param name="reader"></param>
      /// <param name="sourceNo">1 based source number.</param>
      protected LineFeederForSource(TextReader reader, int sourceNo)
      {
         _reader = reader;
         _sourceNo = sourceNo;
      }


      public virtual Tuple<ExternalLine, int> GetNextLine()
      {
         var line = _reader.ReadLine();
         //Note of TextReader.ReadLine behavior at EOF:
         //     Last line unless empty, does not have to be terminated by NewLine (\r\n).
         //     For example, if a file contains SingleLine, it will be treated the same way as SingleLine\r\n;
         //     in either case, 1st ReadLine returns SingleLine and the second one null.
         if (line == null) return null;
         return line.ToExternalTuple(_sourceNo);
      }


      public virtual async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         var line = await _reader.ReadLineAsync();
         if (line == null) return new Tuple<ExternalLine, int>(null, _sourceNo);  //return Tuple<null to indicate end of source (that can be differentiated from null at the end of all sources)
         return line.ToExternalTuple(_sourceNo);
      }


      public virtual void Dispose()
      {
         _reader.Dispose();
      }

   }
}
