using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class removed_src_object_fields_json_from_connectors : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "src_object_fields_json",
                schema: "dedup",
                table: "Connectors");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "src_object_fields_json",
                schema: "dedup",
                table: "Connectors",
                nullable: true);
        }
    }
}
