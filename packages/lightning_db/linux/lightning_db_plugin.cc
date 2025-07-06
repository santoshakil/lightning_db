#include "include/lightning_db/lightning_db_plugin.h"

#include <flutter_linux/flutter_linux.h>
#include <gtk/gtk.h>
#include <sys/utsname.h>

#include <cstring>

#define LIGHTNING_DB_PLUGIN(obj) \
  (G_TYPE_CHECK_INSTANCE_CAST((obj), lightning_db_plugin_get_type(), \
                              LightningDbPlugin))

struct _LightningDbPlugin {
  GObject parent_instance;
};

G_DEFINE_TYPE(LightningDbPlugin, lightning_db_plugin, g_object_get_type())

static void lightning_db_plugin_class_init(LightningDbPluginClass* klass) {}

static void lightning_db_plugin_init(LightningDbPlugin* self) {}

void lightning_db_plugin_register_with_registrar(FlPluginRegistrar* registrar) {
  LightningDbPlugin* plugin = LIGHTNING_DB_PLUGIN(
      g_object_new(lightning_db_plugin_get_type(), nullptr));

  g_object_unref(plugin);
}