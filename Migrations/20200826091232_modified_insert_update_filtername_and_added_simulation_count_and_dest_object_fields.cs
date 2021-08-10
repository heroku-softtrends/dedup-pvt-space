using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class modified_insert_update_filtername_and_added_simulation_count_and_dest_object_fields : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "sync_new_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "sync_update_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.AddColumn<string>(
                name: "dest_object_fields",
                schema: "dedup",
                table: "Connectors",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "simulation_count",
                schema: "dedup",
                table: "Connectors",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "src_new_record_filter",
                schema: "dedup",
                table: "Connectors",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "src_update_record_filter",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "dest_object_fields",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "simulation_count",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_new_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "src_update_record_filter",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.AddColumn<string>(
                name: "sync_new_record_filter",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "sync_update_record_filter",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);
        }
    }
}
