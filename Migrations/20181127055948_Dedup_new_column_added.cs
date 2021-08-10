using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class Dedup_new_column_added : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "compare_config_json",
                schema: "dedup",
                table: "Connectors",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "compare_object_fields",
                schema: "dedup",
                table: "Connectors",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "dedup_source_type",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "compare_config_json",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "compare_object_fields",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "dedup_source_type",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
