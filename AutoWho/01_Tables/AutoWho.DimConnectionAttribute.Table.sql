/*
   Copyright 2016 Aaron Morelli

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
/*
	FILE NAME: AutoWho.DimConnectionAttribute.Table.sql

	TABLE NAME: AutoWho.DimConnectionAttribute

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Holds a distinct list of connection attributes (a subset
	of fields from sys.dm_exec_connections) observed by AutoWho.Collector
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[DimConnectionAttribute](
	[DimConnectionAttributeID] [smallint] IDENTITY(30,1) NOT NULL,
	[net_transport] [nvarchar](40) NOT NULL,
	[protocol_type] [nvarchar](40) NOT NULL,
	[protocol_version] [int] NOT NULL,
	[endpoint_id] [int] NOT NULL,
	[node_affinity] [smallint] NOT NULL,
	[net_packet_size] [int] NOT NULL,
	[encrypt_option] [nvarchar](40) NOT NULL,
	[auth_scheme] [nvarchar](40) NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimConnectionAttributes] PRIMARY KEY CLUSTERED 
(
	[DimConnectionAttributeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON

GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_allattributes] ON [AutoWho].[DimConnectionAttribute]
(
	[net_transport] ASC,
	[protocol_type] ASC,
	[protocol_version] ASC,
	[endpoint_id] ASC,
	[node_affinity] ASC,
	[net_packet_size] ASC,
	[encrypt_option] ASC,
	[auth_scheme] ASC
)
INCLUDE ( 	[DimConnectionAttributeID],
	[TimeAdded]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [AutoWho].[DimConnectionAttribute] ADD  CONSTRAINT [DF_AutoWho_DimConnectionAttributes_TimeAdded]  DEFAULT (getdate()) FOR [TimeAdded]
GO
