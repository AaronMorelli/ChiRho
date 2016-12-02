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
	FILE NAME: AutoWho.DimNetAddress.Table.sql

	TABLE NAME: AutoWho.DimNetAddress

	AUTHOR:			Aaron Morelli
					aaronmorelli@zoho.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/ChiRho

	PURPOSE: Holds a distinct list of IP addresses/ports (from sys.dm_exec_connections)
	observed by AutoWho.Collector
*/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[DimNetAddress](
	[DimNetAddressID] [smallint] IDENTITY(30,1) NOT NULL,
	[client_net_address] [varchar](48) NOT NULL,
	[local_net_address] [varchar](48) NOT NULL,
	[local_tcp_port] [int] NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimNetAddress] PRIMARY KEY CLUSTERED 
(
	[DimNetAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_AutoWho_DimNetAddress] ON [AutoWho].[DimNetAddress]
(
	[client_net_address], 
	[local_net_address],
	[local_tcp_port]
)
INCLUDE ( 	[DimNetAddressID],
	[TimeAdded]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [AutoWho].[DimNetAddress] ADD  CONSTRAINT [DF_AutoWho_DimNetAddress_TimeAdded]  DEFAULT (getdate()) FOR [TimeAdded]
GO
