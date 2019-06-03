//SingleUseBool.cs
//
// Copyright © 2017-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// A simple class that holds X12 delimiter values.
   /// Intended to pass these values from intake to output.
   /// </summary>
   internal class X12Delimiters
   {
      internal string X12SegmentDelimiter;
      internal char X12FieldDelimiter;
   }
}
