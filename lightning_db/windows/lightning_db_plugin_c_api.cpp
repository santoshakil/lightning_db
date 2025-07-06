#include "include/lightning_db/lightning_db_plugin_c_api.h"

#include <flutter/plugin_registrar_windows.h>

#include "lightning_db_plugin.h"

void LightningDbPluginCApiRegisterWithRegistrar(
    FlutterDesktopPluginRegistrarRef registrar) {
  lightning_db::LightningDbPlugin::RegisterWithRegistrar(
      flutter::PluginRegistrarManager::GetInstance()
          ->GetRegistrar<flutter::PluginRegistrarWindows>(registrar));
}
