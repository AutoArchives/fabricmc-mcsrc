import { combineLatest, distinctUntilChanged, map, shareReplay, startWith } from "rxjs";
import { minecraftJar } from "./MinecraftApi";
import { selectedFile } from "./State";

export const hasServerJar = minecraftJar.pipe(
    map(jar => !!jar.serverJar),
    startWith(false),
    distinctUntilChanged(),
    shareReplay({ bufferSize: 1, refCount: true }),
);

export const clientOnlyClass = combineLatest([minecraftJar, selectedFile]).pipe(
    map(([jar, file]) => {
        if (!file || !jar.serverJar) return false;
        return !!jar.jar.entries[file] && !jar.serverJar.entries[file];
    }),
);
