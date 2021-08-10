using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class removesourcetableschemainsertupdatefiltersrccongigjsonconnectortypesyncsrc : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "connector_type",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_config_json",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_new_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_object_name",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_schema",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_update_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "sync_src",
                schema: "dedup",
                table: "Connectors");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "connector_type",
                schema: "dedup",
                table: "Connectors",
                type: "integer",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "src_config_json",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "src_new_record_filter",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "src_object_name",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "src_schema",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "src_update_record_filter",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "sync_src",
                schema: "dedup",
                table: "Connectors",
                type: "integer",
                nullable: false,
                defaultValue: 0);
        }
    }
}
