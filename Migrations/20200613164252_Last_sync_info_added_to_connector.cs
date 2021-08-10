using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class Last_sync_info_added_to_connector : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<DateTime>(
                name: "last_sync_at",
                schema: "dedup",
                table: "Connectors",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "last_sync_status",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "last_sync_at",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "last_sync_status",
                schema: "dedup",
                table: "Connectors");
        }
    }
}
