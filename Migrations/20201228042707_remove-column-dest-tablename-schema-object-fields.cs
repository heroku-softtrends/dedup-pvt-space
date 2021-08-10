using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class removecolumndesttablenameschemaobjectfields : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "dest_object_fields",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "dest_object_name",
                schema: "dedup",
                table: "Connectors");

            migrationBuilder.DropColumn(
                name: "dest_schema",
                schema: "dedup",
                table: "Connectors");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "dest_object_fields",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "dest_object_name",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "dest_schema",
                schema: "dedup",
                table: "Connectors",
                type: "text",
                nullable: true);
        }
    }
}
