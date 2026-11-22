resource "google_project_service" "apis" {
  for_each = local.required_apis

  project = var.project_id
  service = each.value

  # Never disable APIs on destroy: other resources/projects may rely on them.
  disable_on_destroy         = false
  disable_dependent_services = false
}
