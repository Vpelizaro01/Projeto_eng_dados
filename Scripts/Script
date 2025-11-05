/*
=============================
Criar Database e Schemas
=============================

Script simples que cria uma database armazem de dados, apos a checagem se
existe um db com este nome ele dropa  e o recria , em adição o scrip
adiciona três schemas dentro do DB que seriam: Bronze, Prata e Ouro.
*/

-- Drop e recreação do DB ArmazemDeDados

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "ArmazemDeDados")

BEGIN 
    ALTER DATABASE ArmazemDeDados SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	 DROP DATABASE ArmazemDeDados
END;
GO
-- Cria Banco de Dados e Cria Schemas
CREATE DATABASE ArmazemDeDados
GO
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Prata;
GO
CREATE SCHEMA Ouro;
GO

