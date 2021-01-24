//ProductInfo.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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


using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Provides information on this version of Data Conveyer.
   /// </summary>
   public sealed class ProductInfo
   {
      //The property setters below need to be public to allow deserialization
      // via the OrchestratorConfig.RestoreConfig method. Note though that
      // the ProductInfo instance created in RestoreConfig is discarded
      // in the OrchestratorConfig.DataConveyerInfo setter.

      /// <summary>
      /// Name of the component, i.e. DataConveyer.
      /// </summary>
      public string Name { get; set; }
      /// <summary>
      /// Copyright notice.
      /// </summary>
      public string Copyright { get; set; }
      /// <summary>
      /// Version of DataConveyer assembly (major.minor portion of AssemblyVerion).
      /// </summary>
      public string Version { get; set; }
      /// <summary>
      /// FileVersion of DataConveyer assembly (major.minor.build.revision).
      /// </summary>
      public string FileVersion { get; set; }
      /// <summary>
      /// .NET target Data Conveyer was built for.
      /// </summary>
      public string Target { get; set; }
      /// <summary>
      /// Date and Time Data Conveyer was built.
      /// </summary>
      public DateTime Date { get; set; }

      // Singleton pattern
      private static readonly Lazy<ProductInfo> Instance = new Lazy<ProductInfo>(() => new ProductInfo());

      /// <summary>
      /// Current instance of Data Conveyer information.
      /// </summary>
      public static ProductInfo CurrentInfo { get { return Instance.Value; } }
      private ProductInfo()
      {
         var asm = System.Reflection.Assembly.GetExecutingAssembly();
         var asmName = asm.GetName();
         var target = "???undefined???";
         SetNetStandard20(ref target);
         SetNet45(ref target);
         var ver = asmName.Version;

         Name = asmName.Name;
         Copyright = ((AssemblyCopyrightAttribute)asm.GetCustomAttribute(typeof(AssemblyCopyrightAttribute))).Copyright;
         Target = target;
         Version = $"{ver.Major}.{ver.Minor}";
         FileVersion = ((AssemblyFileVersionAttribute)asm.GetCustomAttribute(typeof(AssemblyFileVersionAttribute))).Version;
         Date = File.GetLastWriteTime(asm.Location);
      }

      [Conditional("NETSTANDARD2_0")]
      private void SetNetStandard20(ref string target) { target = "NetStandard2.0"; }

      [Conditional("NET45")]
      private void SetNet45(ref string target) { target = "Net45"; }

      /// <summary>
      /// Return a string representation of the Data Conveyer version.
      /// </summary>
      /// <returns>E.g. DataConveyer v2.6 for NetStandard2.0 (build 2.6.3.15683 from 03-15-2019 at 10:47:24 AM)...</returns>
      public override string ToString()
      {
         return string.Format("{0} v{1} for {2} (Build {3} from {4:MM-dd-yyyy a\\t hh:mm:ss tt})\r\n{5}",
                              this.Name, this.Version, this.Target, this.FileVersion, this.Date, this.Copyright);
      }
   }
}
