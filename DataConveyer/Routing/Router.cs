//Router.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;

namespace Mavidian.DataConveyer.Routing
{
   /// <summary>
   /// Provides Route function to determine target number (Strategy Pattern)
   /// </summary>
   internal abstract class Router
   {
      /// <summary>
      /// Factory method that returns an instance of a concrete Router (inherited class)
      /// </summary>
      /// <param name="config"></param>
      /// <returns></returns>
      internal static Router CreateRouter(OrchestratorConfig config)
      {
         //Note that SingleTarget and SourceToTarget router types are special cases of PerCluster and PerRecord respectively,
         // so that SingleTargetRouter and SourceToTargetRouter could be substituted by PerClusterRouter and PerRecordRouter
         // classes respectively (e.g. PerClusterRouter((c) => 1)). But, specialized classes perform a hair faster. 
         switch (config.RouterType)
         {
            case RouterType.SingleTarget:
               return new SingleTargetRouter();
            case RouterType.SourceToTarget:
               return new SourceToTargetRouter();
            case RouterType.PerCluster:
               return new PerClusterRouter(config.ClusterRouter);
            case RouterType.PerRecord:
               return new PerRecordRouter(config.RecordRouter);
            default:
               //TODO: Message - fatal error, invalid router type
               return null;
         }
      }

      /// <summary>
      /// Determine routing info (TargetNo) for all record in a cluster.
      /// </summary>
      /// <param name="cluster">Cluster to be updated with routing info.</param>
      /// <returns>Cluster with updated routing info, i.e. TargetNo.</returns>
      internal abstract ICluster Route(ICluster cluster);
   }
}
