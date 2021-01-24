//X12FeederForSource.cs
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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// X12 segment reader (from a single source).
   /// </summary>
   internal class X12FeederForSource : LineFeederForSource
   {
      private char _segmentDelimiterChar;

      /// <summary>
      /// Creates an instance given a stream reader (from a specific text file) a source number and a boolean flag to skip first row.
      /// </summary>
      /// <param name="reader"></param>
      /// <param name="sourceNo">1 based source number.</param>
      /// <param name="x12SegmentDelimiter">Segment delimiter (may include whitespace, e.g. "~\r\n").</param>
      internal X12FeederForSource(TextReader reader, int sourceNo, string x12SegmentDelimiter) : base(reader, sourceNo)
      {
         Debug.Assert(x12SegmentDelimiter != null);
         _segmentDelimiterChar = x12SegmentDelimiter[0];
      }


      public override Tuple<ExternalLine, int> GetNextLine()
      {
         return LineAsSegment(ReadSegment());
      }


      public override async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         return LineAsSegment(await ReadSegmentAsync());
      }


      /// <summary>
      /// Read a "line" based on the given delimiter (applicable to X12 segment)
      /// </summary>
      /// <returns></returns>
      private string ReadSegment()
      {
         List<char> chars = new List<char>();
         bool before1stLetter = true; //to remove leading whitespace (common when CR is segment delimiter and is followed by LF)
         var currChar = ReadNextChar();
         int cnt = 1;
         while (currChar != _segmentDelimiterChar)
         {
            if (before1stLetter && char.IsWhiteSpace(currChar))
            {
               currChar = ReadNextChar();
               continue;
            }
            before1stLetter = false;
            if (currChar == '\0') return chars.Any() ? new String(chars.ToArray()) : null; //end of stream
            chars.Add(currChar);
            if (cnt == 3 && chars[0] == 'I' && chars[1] == 'S' && chars[2] == 'A')
            { //ISA segment encountered, read the remaining 103 characters (ISA has a total of 106), instead to segment delimiter
               var buffer = new char[103];
               _reader.Read(buffer, 0, 103);
               chars = chars.Concat(buffer).ToList();
               break;
            }
            currChar = ReadNextChar();
            cnt++;
         }
         return new String(chars.ToArray());
      }


      /// <summary>
      /// Read next character from text reader
      /// </summary>
      /// <returns>Character read or \0 if no more characters are available</returns>
      private char ReadNextChar()
      {
         var curr = _reader.Read();
         return curr == -1 ? '\0' : (char)curr;
      }


      /// <summary>
      /// Asynchronously read a "line" based on the given delimiter (applicable to X12 segment)
      /// </summary>
      /// <returns></returns>
      private async Task<string> ReadSegmentAsync()
      {
         List<char> chars = new List<char>();
         bool before1stLetter = true; //to remove leading whitespace (common when CR is segment delimiter and is followed by LF)
         var currChar = await ReadNextCharAsync();
         int cnt = 1;
         while (currChar != _segmentDelimiterChar)
         {
            if (before1stLetter && char.IsWhiteSpace(currChar))
            {
               currChar = await ReadNextCharAsync();
               continue;
            }
            before1stLetter = false;
            if (currChar == '\0') return chars.Any() ? new String(chars.ToArray()) : null; //end of stream
            chars.Add(currChar);
            if (cnt == 3 && chars[0] == 'I' && chars[1] == 'S' && chars[2] == 'A')
            { //ISA segment encountered, read the remaining 103 characters (ISA has a total of 106), instead to segment delimiter
               var buffer = new char[103];
               await _reader.ReadAsync(buffer, 0, 103);
               chars = chars.Concat(buffer).ToList();
               break;
            }
            currChar = await ReadNextCharAsync();
            cnt++;
         }
         return new String(chars.ToArray());
      }


      /// <summary>
      /// Asynchronously read next character from text reader
      /// </summary>
      /// <returns>Task with character read or \0 if no more characters are available</returns>
      private async Task<char> ReadNextCharAsync()
      {
         var buffer = new char[1];
         int n = await _reader.ReadAsync(buffer, 0, 1);
         return n == 0 ? '\0' : buffer[0];
      }


      /// <summary>
      /// Translate a line of text (X12 segment) to a tuple containing segment, but don't translate null (EOD mark)
      /// Side-effect - in case of ISA segment, remember segment delimiter (to be used in reading subsequent segments)
      /// </summary>
      /// <param name="line"></param>
      /// <returns></returns>
      private Tuple<ExternalLine, int> LineAsSegment(string line)
      {
         if (line == null) return null;
         if (line.StartsWith("ISA")) _segmentDelimiterChar = line[105];
         //TODO: Make the tuple of Xsegment type (not Xtext)
         return line.ToExternalTuple(base._sourceNo);
      }

   }
}

